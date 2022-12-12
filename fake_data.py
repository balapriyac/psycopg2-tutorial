from faker import Faker

fake = Faker()
Faker.seed(42)

def generate_fake_data(num):
    records = []

    for i in range(num):
        name, city, job = fake.name(), fake.city(), fake.job()
        records.append((name,city,job))
    return records
