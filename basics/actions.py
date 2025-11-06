from transformations import transformations_example

def run_actions():
    rdds = transformations_example()

    print("Unique order statuses:")
    print(rdds["unique_statuses"].collect())

    print("\nDelivered Orders:")
    for row in rdds["delivered_orders"].take(3):
        print(row)

    print("\nOrder count per customer:")
    print(rdds["orders_per_customer"].collect())

    print("\nTop customers:")
    print(rdds["sorted_customers"].take(3))

    print("\nWord count sample (flatMap test):")
    print(rdds["words"].take(10))

if __name__ == "__main__":
    run_actions()
