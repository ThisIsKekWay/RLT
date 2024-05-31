import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import json
from database.config import init
from database.model import SampleCollection


async def get_payload(payload: str) -> dict:
    aggregation_payload = json.loads(payload)
    return aggregation_payload


async def aggregate_pool(data: dict) -> str:
    dt_from = datetime.strptime(data["dt_from"], '%Y-%m-%dT%H:%M:%S')
    dt_upto = datetime.strptime(data["dt_upto"], '%Y-%m-%dT%H:%M:%S')
    if dt_from > dt_upto:
        dt_from, dt_upto = dt_upto, dt_from
    group_type = data["group_type"]
    payments = await SampleCollection.find(
        SampleCollection.dt >= dt_from,
        SampleCollection.dt <= dt_upto
    ).to_list()
    if group_type == "month":
        return await aggregate_month(payments, dt_from, dt_upto)
    elif group_type == "day":
        return await aggregate_day(payments, dt_from, dt_upto)
    elif group_type == "hour":
        return await aggregate_hour(payments, dt_from, dt_upto)
    else:
        raise ValueError("Неверный тип группировки")


async def aggregate_month(payments, dt_from: datetime, dt_upto: datetime):
    monthly_payments = defaultdict(int)

    for payment in payments:
        month_key = payment.dt.month
        monthly_payments[month_key] += payment.value

    dataset = list(monthly_payments.values())
    labels = [f"{dt_from.year}-{str(month_key).zfill(2)}-01T00:00:00" for month_key in monthly_payments.keys()]

    return json.dumps({"dataset": dataset, "labels": labels})


async def aggregate_hour(payments: list, dt_from: datetime, dt_upto: datetime):
    daily_hourly_payments = defaultdict(lambda: defaultdict(int))

    for payment in payments:
        day = payment.dt.date()
        hour = payment.dt.hour
        daily_hourly_payments[day][hour] += payment.value

    hourly_dataset = []
    hourly_labels = []

    current_day = dt_from.date()

    while current_day <= dt_upto.date():
        for hour in daily_hourly_payments[day]:
            hourly_dataset.append(daily_hourly_payments[current_day][hour])
            hourly_labels.append(f"{current_day}T{str(hour).zfill(2)}:00:00")

        current_day += timedelta(days=1)

    return json.dumps({"dataset": hourly_dataset[:25], "labels": hourly_labels[:25]})


async def aggregate_day(payments: list, dt_from: datetime, dt_upto: datetime):
    daily_payments = defaultdict(int)
    current_date = dt_from
    while current_date <= dt_upto:
        daily_payments[(current_date.month, current_date.day)] = sum(
            payment.value for payment in payments if payment.dt.date() == current_date.date()
        )
        current_date += timedelta(days=1)

    daily_dataset = list(daily_payments.values())
    daily_labels = [f"{dt_from.year}-{str(month).zfill(2)}-{str(day).zfill(2)}T00:00:00" for month, day in daily_payments.keys()]

    return json.dumps({"dataset": daily_dataset, "labels": daily_labels})


data = '{"dt_from":"2022-09-01T00:00:00", "dt_upto":"2022-12-31T23:59:00", "group_type":"month"}'
# '{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}'
# '{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}'


async def main():
    await init()
    parsed_payload = await get_payload(data)
    aggregated_payments = await aggregate_pool(parsed_payload)
    print(aggregated_payments)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
