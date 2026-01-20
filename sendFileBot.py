import logging
import json
# import sqlite3
import re
import asyncio
import uuid
import aiohttp
import os
from dotenv import load_dotenv
import redis.asyncio as redis
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

load_dotenv()

# import json
# import re
# import requests
# import asyncio
import random
# import nest_asyncio
from io import BytesIO
from PIL import Image, ImageFilter
# from telegram import Bot
# from telegram.ext import Application, MessageHandler, filters

# --- CONFIG ---
TOKEN = "8456161392:AAEDRf0zt2-DNLRE4l_49da091P0UL-Ollo"
STORAGE_CHANNEL_ID = -1003626010165 # Channel where source files exist
PUBLIC_CH = -1001554146160   
LOG_CHANNEL_ID = -1003612771904    # Private channel for Admin Logs
BOT_USERNAME = "dailyviralbot"
SHRINKME_API_KEY = "ce30f228d066eee6c98589e9cf3cc6dede712f46" # Replace with user's key if provided, else placeholder from prompt
ADMIN_IDS = [5668673752] # Replace with actual admin ID
# ADMIN_IDS = [1582144043]
bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- DATABASE SETUP ---
# --- DATABASE SETUP ---
# Initialize Redis
r_conn = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

async def init_db():
    try:
        await r_conn.ping()
        print("✅ Redis Connected Successfully")
    except Exception as e:
        print(f"❌ Redis Connection Failed: {e}")

async def save_video(id_num, msg_id):
    # Key: video:{short_id} -> message_id
    await r_conn.set(f"video:{id_num}", msg_id)

async def get_msg_id(id_num):
    msg_id = await r_conn.get(f"video:{id_num}")
    return int(msg_id) if msg_id else None

# --- USER MANAGEMENT ---
def get_seconds_until_midnight():
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((midnight - now).total_seconds())

async def check_user_status(user_id):
    # Keys:
    # user:{id}:usage (Int)
    # user:{id}:verified (Bool/1)
    # user:{id}:token (String)
    
    usage_key = f"user:{user_id}:usage"
    verify_key = f"user:{user_id}:verified"
    token_key = f"user:{user_id}:token"

    usage_count = await r_conn.get(usage_key) or 0
    is_verified = await r_conn.get(verify_key)
    token = await r_conn.get(token_key)

    return {
        "usage_count": int(usage_count),
        "is_verified": bool(is_verified),
        "verification_token": token
    }

async def update_usage(user_id):
    usage_key = f"user:{user_id}:usage"
    
    # Increment usage
    new_val = await r_conn.incr(usage_key)
    
    # If first usage (val=1), set expiry to midnight
    if new_val == 1:
        await r_conn.expire(usage_key, get_seconds_until_midnight())

async def set_verified(user_id):
    verify_key = f"user:{user_id}:verified"
    await r_conn.set(verify_key, 1)
    await r_conn.expire(verify_key, get_seconds_until_midnight())

async def save_verification_token(user_id, token):
    token_key = f"user:{user_id}:token"
    await r_conn.set(token_key, token)
    # Token valid for 24h or until midnight? Let's say until midnight to match cycle
    await r_conn.expire(token_key, get_seconds_until_midnight())

# --- CHANNEL MANAGEMENT ---
# Storing channels in a Redis Hash: channels_map -> {channel_id: json_data}
async def add_channel(channel_id, link, name):
    data = json.dumps({"link": link, "name": name})
    await r_conn.hset("channels_map", str(channel_id), data)

async def remove_channel(channel_id):
    await r_conn.hdel("channels_map", str(channel_id))

async def get_channels():
    all_channels = await r_conn.hgetall("channels_map")
    result = []
    for ch_id, ch_data in all_channels.items():
        data = json.loads(ch_data)
        result.append({"id": ch_id, "link": data["link"], "name": data["name"]})
    return result

async def process_image(url):
    """Downloads the image asynchronously and blurs the ENTIRE thing in a thread."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # Async download
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status != 200:
                    return None
                data = await response.read()

        def blocking_blur(image_data):
            img = Image.open(BytesIO(image_data)).convert("RGB")
            
            # 1. Apply heavy blur to the WHOLE image
            img = img.filter(ImageFilter.GaussianBlur(radius=25))
            
            # 2. Add Noise layer
            width, height = img.size
            pixels = img.load()
            for _ in range(int(width * height * 0.15)): 
                x, y = random.randint(0, width - 1), random.randint(0, height - 1)
                pixels[x, y] = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
            
            bio = BytesIO()
            img.save(bio, 'JPEG', quality=80)
            bio.seek(0)
            return bio

        # Run CPU-bound task in executor
        loop = asyncio.get_running_loop()
        bio = await loop.run_in_executor(None, blocking_blur, data)
        return bio
        
    except Exception as e:
        print(f"Blur Error: {e}")
        return None


# --- AUTO DELETE TASK ---
async def delete_after_delay(chat_id: int, message_id: int, delay: int = 1800):
    """Deletes a message after a specified delay (default 30 mins / 1800s)."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest:
        pass # Message already deleted or bot has no permission

# --- HELPERS ---
async def shorten_url(destination_url):
    """Shortens a URL using ShrinkMe.io API."""
    api_url = f"https://shrinkme.io/api?api={SHRINKME_API_KEY}&url={destination_url}&alias={uuid.uuid4().hex[:8]}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url) as resp:
                data = await resp.json()
                if data["status"] == "success":
                    return data["shortenedUrl"]
        except Exception as e:
            logging.error(f"Error shortening URL: {e}")
            return destination_url # Fallback

async def check_subscription(user_id, bot):
    """Checks if user is subscribed to all forced channels."""
    channels = await get_channels()
    not_joined = []
    
    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch["id"], user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_joined.append(ch)
        except Exception as e:
            logging.error(f"Error checking sub for {ch['id']}: {e}")
            # If bot isn't admin or can't check, assume joined or ignore to avoid blocking
            continue
            
    return not_joined

# --- HANDLERS ---

# Admin Commands for Channel Management
@dp.message(Command("addch"))
async def add_channel_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = command.args
    if not args:
        await message.answer("Usage: /addch <channel_id> <link> <name>")
        return
    
    parts = args.split(' ', 2)
    if len(parts) < 3:
        await message.answer("Usage: /addch <channel_id> <link> <name>")
        return
        
    await add_channel(parts[0], parts[1], parts[2])
    await message.answer(f"Channel {parts[2]} added.")

@dp.message(Command("delch"))
async def del_channel_cmd(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
        
    args = command.args
    if not args:
        await message.answer("Usage: /delch <channel_id>")
        return
        
    await remove_channel(args)
    await message.answer("Channel removed.")

@dp.channel_post(F.chat.id == STORAGE_CHANNEL_ID)
async def watch_channel(message: types.Message):
    if message.caption:
        match = re.search(r"video\s#(\d+)", message.caption.lower())
        json_match = re.search(r'\{.*\}', message.caption, re.DOTALL)
        if match:
            id_num = match.group(1)
            data = json.loads(json_match.group(0))
            await save_video(id_num, message.message_id)

            name = data.get("name", "Unknown Title")
            raw_dur = data.get("duration", "0S")
            duration = raw_dur.replace("P0DT0H", "").replace("M", "m ").replace("S", "s")
            thumb_url = data.get("thumbnail")
            
            # Process fully blurred image
            processed_photo_io = await process_image(thumb_url)
        
            banner_text = (
            f"🎬 **{name}**\n\n"
            f"⏳ **duration:-** {duration}\n"
            f"📥 **download link:-** https://t.me/{BOT_USERNAME}?start={id_num}\n\n"
            f"🚀 *Share us to support!*"
            )
        
            if processed_photo_io:
                photo_file = types.BufferedInputFile(processed_photo_io.getvalue(), filename="blur.jpg")
                await bot.send_photo(
                    chat_id=PUBLIC_CH,
                    photo=photo_file,
                    caption=banner_text,
                    parse_mode="Markdown"
                )
                print(f"✅ Full-Blur Banner posted for ID: {id_num}")
            else:
                print(f"❌ Failed to process image for ID: {id_num}")
        
            # Log with start link
            log_data = {
                "event": "new_video_indexed",
                "id": id_num,
                "msg_id": message.message_id,
                "start_link": f"https://t.me/{BOT_USERNAME}?start={id_num}"
            }
            await bot.send_message(LOG_CHANNEL_ID, f"{json.dumps(log_data, indent=2)}", parse_mode="HTML")


# Callback for the "Delete Now" button
@dp.callback_query(F.data == "delete_msg")
async def delete_button_handler(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        await callback.answer("Message already deleted.")

# Callback for re-checking subscription
@dp.callback_query(F.data.startswith("check_sub_"))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    original_args = callback.data.replace("check_sub_", "")
    
    not_joined = await check_subscription(user_id, bot)
    
    if not not_joined:
        await callback.message.delete()
        await callback.answer("✅ Verified! Processing your request...")
        # Cleanly trigger start logic again
        # We can't directly call cmd_start easily due to message obj diffs, so we simulate logic
        # Or constructing a fake message. Easier to just tell user to click start again or do manual call.
        # But user expects seamless flow.
        # Let's call a shared helper or just instruct user.
        # Ideally, we reinvoke the logic.
        
        # HACK: Construct a fake message object to call cmd_start
        # NOTE: This is slightly risky but effective for keeping logic in one place
        # Alternatively, refactor logic.
        await callback.message.answer(f"✅ Verified! Click here: /start {original_args}")
        
    else:
        await callback.answer("❌ You haven't joined all channels yet!", show_alert=True)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject):
    args = command.args
    user_id = message.from_user.id
    
    # 1. Delete the /start command from user immediately
    try:
        await message.delete()
    except TelegramBadRequest:
        pass
        
    # 2. Force Subscribe Check
    not_joined = await check_subscription(user_id, bot)
    if not_joined:
        builder = InlineKeyboardBuilder()
        for ch in not_joined:
            builder.row(types.InlineKeyboardButton(text=f"Join {ch['name']}", url=ch['link']))
        
        # Pass args to callback so we resume correct video
        callback_arg = args if args else ""
        builder.row(types.InlineKeyboardButton(text="Try Again 🔄", callback_data=f"check_sub_{callback_arg}"))
        
        await message.answer("⚠️ You must join our channels to access files.", reply_markup=builder.as_markup())
        return

    if not args:
        await message.answer("Welcome! Send a valid ID to get a video.")
        return
    
    # 3. Handle Verification Return (start=verify_TOKEN)
    if args.startswith("verify_"):
        token = args.replace("verify_", "")
        user_data = await check_user_status(user_id)
        
        if user_data["verification_token"] == token:
            await set_verified(user_id)
            await message.answer("✅ You are successfully verified! You can now access unlimited videos for today.")
        else:
            await message.answer("❌ Invalid or expired verification link.")
        return

    # 4. Handle Video Request (start=123)
    msg_id_in_storage = await get_msg_id(args)
    
    if msg_id_in_storage:
        user_data = await check_user_status(user_id)
        
        # CHECK LIMITS
        # - Pass if usage < 3
        # - Pass if verified
        if user_data["usage_count"] < 3 or user_data["is_verified"]:
            
            start_link = f"https://t.me/{BOT_USERNAME}?start={args}"
            
            # Create Keyboard
            builder = InlineKeyboardBuilder()
            builder.row(types.InlineKeyboardButton(text="Share Video 🔗", url=f"https://t.me/share/url?url={start_link}"))
            builder.row(types.InlineKeyboardButton(text="Delete Now 🗑️", callback_data="delete_msg"))

            # Send the file
            sent_video = await bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=msg_id_in_storage,
                reply_markup=builder.as_markup(),
                caption="video is here"
            )
            
            # Increment Usage
            await update_usage(user_id)
            
            # Log request
            log_data = {
                "event": "#user_request",
                "user_id": message.from_user.id,
                "requested_id": args,
                "status": "sent"
            }
            await bot.send_message(LOG_CHANNEL_ID, f"{json.dumps(log_data, indent=2)}", parse_mode="HTML")

            # Schedule auto-deletion (900s)
            asyncio.create_task(delete_after_delay(message.chat.id, sent_video.message_id, 900))
            
        else:
            # LIMIT REACHED -> Generate Ad Link
            new_token = uuid.uuid4().hex
            await save_verification_token(user_id, new_token)
            
            dest_link = f"https://t.me/{BOT_USERNAME}?start=verify_{new_token}"
            final_ad_link = await shorten_url(dest_link)
            
            await message.answer(
                f"⚠️ <b>Daily Limit Reached (3/3)</b>\n\nTo access more files today, please complete this verification:\n\n👉 {final_ad_link}\n\n<i>This helps keep our bot free!</i>",
                parse_mode="HTML"
            )
            
    else:
        temp_msg = await message.answer("Video not found.")
        asyncio.create_task(delete_after_delay(message.chat.id, temp_msg.message_id, 10))

# Listener is already defined above

async def main():
    await init_db()
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())