while True:
    try:
        if "transaction_index" in locals():
            last_transaction_index = transaction_index
            print(f"\nLast transaction index: {transaction_index}")
        transaction_index = input(
            "Select row you would like to categorize.\nEnter `s` to save and quit if this looks good.\nPress Enter to move to categorize next transaction.\nEnter `q` to quit without saving.\n"
        )
        if transaction_index == "":
            if "last_transaction_index" in locals():
                transaction_index = last_transaction_index + 1
            else:
                transaction_index = 0
        elif transaction_index == "s":
            transaction_index = -1
        elif transaction_index == "q":
            transaction_index = -2
        transaction_index = int(transaction_index)
        if transaction_index >= 0:
            self.data_to_categorize.loc[transaction_index]
        break
    except KeyboardInterrupt:
        exit()
    except:
        print("Not an integer value or out of range...")

