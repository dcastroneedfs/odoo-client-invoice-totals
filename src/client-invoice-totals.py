import time
print("🐍 Render worker script started successfully!")

i = 0
while True:
    print(f"🟢 Heartbeat... {i}")
    time.sleep(10)
    i += 1
