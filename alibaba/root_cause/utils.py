from datetime import datetime
import pytz,time
tz=pytz.timezone('Asia/Shanghai')
def timestamp_to_date(timestamp):
    # try:
    #     return datetime.fromtimestamp(timestamp,tz).strftime('%m-%d_%H:%M')
    # except:
        return datetime.fromtimestamp((timestamp//1000),tz).strftime('%m-%d_%H:%M')
    # except:
    #     return datetime.fromtimestamp(timestamp//1000).strftime('%m-%d_%H:%M')
def transfer_date_to_timestamp(date_str):
    return int(time.mktime(datetime.strptime('2019-%s'%(date_str), '%Y-%m-%d_%H:%M').replace(tzinfo=tz).astimezone(pytz.utc).timetuple()))*1000 + 6*60*1000 #add 6mins
