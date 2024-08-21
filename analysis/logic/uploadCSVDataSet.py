

import pandas as pd
from analysis.models import Product
from functools import reduce

class UploadCleanDataSet:

    def __init__(self) -> None:
        self.columns = Product._meta.get_fields()

    @staticmethod
    def CleanDataSetAsPerGivenRules(df: pd.DataFrame)->pd.DataFrame:
        df['price'].fillna(df['price'].median(), inplace=True)
        df['quantity_sold'].fillna(df['quantity_sold'].median(), inplace=True)
        #Fill The Nan Values With Category Wise Mean Value
        df['rating'] = df.groupby('category')['rating'].transform(lambda x: x.fillna(x.mean()))
        return df
    
    @staticmethod
    def ConvertToKeyValueHasMap(df: pd.DataFrame) -> dict:
        return reduce(
            lambda acc, product:{**acc, product["product_id"]:product},
            df,
            {}
        )
    
    def processDataSetToDatabase(self, path):
        df_dict = self.ConvertToKeyValueHasMap(df=self.cleanDataSetAsPerGivenRules(df=pd.read_csv(path)))
        UpdatedSet = set()
        products = Product.objects.filter(id__in=list(df_dict.keys()))
        for product in enumerate(products):
            UpdatedSet.add(product.id)
            UpdateDetails = df_dict[product.id]
            UpdateAbleFields = set(self.columns).intersection(set(UpdateDetails.keys()))
            for column in UpdateAbleFields:
                setattr(product, column, UpdateDetails[column])
        
        BULK_ADD = []
        for newProduct in df_dict.keys()- UpdatedSet:
            BULK_ADD.append(
                Product(**df_dict[newProduct])
            )
        
        if products:
            Product.objects.bulk_update(products, fields=self.columns, batch_size=20)

        if BULK_ADD:
            Product.objects.bulk_create(BULK_ADD, batch_size=30)






