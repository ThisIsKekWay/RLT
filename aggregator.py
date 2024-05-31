import asyncio
from collections import defaultdict
from datetime import datetime
import json
from database.config import init
from database.model import SampleCollection


async def get_payload(payload: str) -> dict:
    aggregation_payload = json.loads(payload)
    return aggregation_payload


async def aggregate_pool(data: dict) -> dict:
    dt_from = datetime.strptime(data["dt_from"], '%Y-%m-%dT%H:%M:%S')
    dt_upto = datetime.strptime(data["dt_upto"], '%Y-%m-%dT%H:%M:%S')
    if dt_from > dt_upto:
        dt_from, dt_upto = dt_upto, dt_from
    group_type = data["group_type"]
    payments = await SampleCollection.find(
        SampleCollection.dt >= dt_from,
        SampleCollection.dt <= dt_upto
    ).to_list()

    monthly_payments = defaultdict(int)

    for payment in payments:
        month_key = payment.dt.month
        monthly_payments[month_key] += payment.value

    dataset = list(monthly_payments.values())
    labels = [f"{dt_from.year}-{str(month_key).zfill(2)}-01T00:00:00" for month_key in monthly_payments.keys()]

    return {"dataset": dataset, "labels": labels}


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
