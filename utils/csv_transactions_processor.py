import pandas as pd


class CsvTransactionsProcessor:
    def __init__(self, csv="./transactions.csv") -> None:
        self.csv = csv
        df = pd.read_csv(csv)
        df["Booking Date"] = pd.to_datetime(df["Booking Date"])
        self.df = df.copy()

        df.set_index("Booking Date", inplace=True)
        credit = df[df["Credit Debit Indicator"] == "Credit"]
        credit_monthly = credit.resample("M").sum()

        debit = df[df["Credit Debit Indicator"] == "Debit"]
        debit_monthly = debit.resample("M").sum()

        self.difference_monthly = credit_monthly["Amount"].sub(
            debit_monthly["Amount"], axis=0
        )

    def get_month_total(self, month):
        """
        Get the total for a specific month from the difference_monthly DataFrame.

        Parameters:
        month (int): The month number (1-12).

        Returns:
        DataFrame: A DataFrame containing the data for the specified month.
        """
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
        return self.difference_monthly.loc[self.difference_monthly.index.month == month]

    def sum_of_last_months(self, months):
        """
        Calculate the sum of the last 'months' entries in the difference_monthly DataFrame.

        Parameters:
        months (int): The number of last months to sum.

        Returns:
        float: The sum of the last 'months' entries, rounded to 2 decimal places.
        """
        if months < 1:
            raise ValueError("Months must be greater than 0")
        return round(self.difference_monthly[-months:].sum(), 2)

    def overall_sum(self):
        """
        Calculate the overall sum of the difference_monthly DataFrame.

        Returns:
        float: The sum of all entries in difference_monthly, rounded to 2 decimal places.
        """
        return round(self.difference_monthly.sum(), 2)

    def sum_by_category(self, month):
        """
        Calculate the sum of expenses by category by a given month.

        Parameters:
        month (int): The month number (1-12).

        Returns:
        DataFrame: A DataFrame containing the sum of expenses by category.
        """
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
        return (
            self.df[self.df["Booking Date"].dt.month == month]
            .groupby("Category")["Amount"]
            .sum()
        )

    def get_available_years(self):
        """
        Get the list of available years in the DataFrame.

        Returns:
        list: A list of available years.
        """
        return self.difference_monthly.index.year.unique().tolist()
