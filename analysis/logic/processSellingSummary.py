from analysis.models import Product
import pandas as pd

class SummaryReportProcessor:

    @staticmethod
    def loadDataFrame() -> pd.DataFrame:
        try:
            products = Product.objects.all().values()
            return pd.DataFrame(products)
        except Exception as e:
            raise Exception(e)

    def processSumamry(self):
        try:
            df = self.loadDataFrame()
            summary = df.groupby('category').agg(
                total_revenue=pd.NamedAgg(column='price', aggfunc='sum'),
                top_product=pd.NamedAgg(column='product_name', aggfunc=lambda x: df.loc[df['quantity_sold'].idxmax(), 'product_name']),
                top_product_quantity_sold=pd.NamedAgg(column='quantity_sold', aggfunc='max')
            ).reset_index()
            summary.to_csv('Summary_Report.csv', index=False)
            return {'message': 'Summary report generated successfully'}
        except Exception as e:
            raise Exception(e)
