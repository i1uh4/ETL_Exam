import csv, random, datetime as dt
regions = ["DE-HE","DE-BY","DE-BE","RU-MOW","RU-SPE"]
statuses = ["answered","missed","busy"]
campaigns = ["credit_card_offer","loan_offer","deposit_offer"]
responses = ["interested","not_interested","callback"]

with open("transactions_v2.csv","w",newline="") as f:
    w = csv.writer(f)
    w.writerow(["call_id","call_time","client_id","region_code",
                "campaign_type","call_status","client_response",
                "duration_sec","follow_up_required"])
    for i in range(600_000):  # ~30+ MB
        w.writerow([
            f"call_{i:08d}",
            (dt.datetime(2026,5,1)+dt.timedelta(seconds=i)).isoformat(),
            f"client_{random.randint(1,9999)}",
            random.choice(regions),
            random.choice(campaigns),
            random.choice(statuses),
            random.choice(responses),
            random.randint(5,600),
            random.choice([True,False])
        ])