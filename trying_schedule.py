from schedule import every, repeat, run_pending
import time
from datetime import datetime

@repeat(every(2).minutes)
def job():
    try:
        now = datetime.now()
        print(hola)
    except Exception as e:
        print("Hubo un error, pero continuo")


while True:
    run_pending()
    time.sleep(1)
