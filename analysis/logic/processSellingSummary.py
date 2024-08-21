from analysis.models import Product
import pandas as pd

class SummaryReportProcessor:

    @staticmethod
    def loadDataFrame() -> pd.DataFrame:
        try:
            # Load all products from the database and convert them to a pandas DataFrame.
            products = Product.objects.all().values()
            return pd.DataFrame(products)
        except Exception as e:
            # Handle any exception that occurs during data retrieval or DataFrame creation.
            raise Exception(f"Error loading data into DataFrame: {e}")

    def processSummary(self):
        try:
            # Load the DataFrame using the loadDataFrame method.
            df = self.loadDataFrame()

            # Generate a summary report by grouping data by 'category'.
            summary = df.groupby('category').agg(
                total_revenue=pd.NamedAgg(column='price', aggfunc='sum'),
                top_product=pd.NamedAgg(
                    column='product_name', 
                    aggfunc=lambda x: df.loc[df['quantity_sold'].idxmax(), 'product_name']
                ),
                top_product_quantity_sold=pd.NamedAgg(column='quantity_sold', aggfunc='max')
            ).reset_index()

            # Save the summary report to a CSV file.
            summary.to_csv('Summary_Report.csv', index=False)
            return {'message': 'Summary report generated successfully'}
        except KeyError as e:
            # Handle missing columns during the aggregation process.
            raise Exception(f"KeyError during summary processing: Missing expected column {e}")
        except pd.errors.EmptyDataError:
            # Handle cases where the DataFrame is empty.
            raise Exception("DataFrame is empty. No data available to process the summary report.")
        except Exception as e:
            # Catch all other exceptions that may occur.
            raise Exception(f"Unexpected error during summary processing: {e}")
