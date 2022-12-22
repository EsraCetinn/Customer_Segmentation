# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 16:52:30 2022

@author: USER
"""

import datetime as dt
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.4f' %x)

################################
#master_id: Eşsiz müşteri numarası
#order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
#last_order_channel: En son alışverişin yapıldığı kanal
#first_order_date: Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date: Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi
#order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret
#interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi
#################################

#GÖREV 1

#Adım 1
df_=pd.read_csv('flo_data_20k.csv')
df=df_.copy()

#Adım2
df.head()
df.shape
df.isnull().sum()
df.describe().T


df["order_num_total"]=df["order_num_total_ever_online"]+df["order_num_total_ever_offline"]
df["customer_value_total"]=df["customer_value_total_ever_online"]+df["customer_value_total_ever_offline"]

df.head()

df.groupby("master_id").agg({"order_num_total":"sum"}).sort_values("order_num_total",ascending=False).head()
df.groupby("master_id").agg({"customer_value_total":"sum"}).sort_values("customer_value_total",ascending=False).head()


df.info()
df.first_order_date=df.first_order_date.apply(pd.to_datetime)
df.last_order_date=df.last_order_date.apply(pd.to_datetime)
df.last_order_date_online=df.last_order_date_online.apply(pd.to_datetime)
df.last_order_date_offline=df.last_order_date_offline.apply(pd.to_datetime)

df.info()

df.describe().T

df.groupby("order_channel").agg({"master_id":"count"})
df.groupby("order_channel").agg({"order_num_total":"sum"})
df.groupby("order_channel").agg({"customer_value_total":"sum"})

# df.groupby("order_channel").agg({"master_id":"count",
#                                  "order_num_total":"sum",
#                                  "customer_value_total":"sum"})


df.groupby("master_id").agg({"customer_value_total":"sum"}).sort_values("customer_value_total", ascending=False).head(10)
# df.sort_values("customer_value_total", ascending=False)[:10]

df.groupby("master_id").agg({"order_num_total":"sum"}).sort_values("order_num_total", ascending=False).head(10)
# df.sort_values("order_num_total", ascending=False)[:10]


def preparation(dataframe):

    dataframe["order_num_total"]=dataframe["order_num_total_ever_online"]+dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"]=dataframe["customer_value_total_ever_online"]+dataframe["customer_value_total_ever_offline"]

    dataframe.first_order_date=dataframe.first_order_date.apply(pd.to_datetime)
    dataframe.last_order_date=dataframe.last_order_date.apply(pd.to_datetime)
    dataframe.last_order_date_online=dataframe.last_order_date_online.apply(pd.to_datetime)
    dataframe.last_order_date_offline=dataframe.last_order_date_offline.apply(pd.to_datetime)

    return df


df=df_.copy()
df_prepared=preparation(df)

df_prepared.head()


#GÖREV 2

df.head()
df["last_order_date"].max()
analysis_date=dt.datetime(2021,6,1)
type(analysis_date)

rfm = pd.DataFrame()
rfm["customer_id"] = df["master_id"]
rfm["recency"] = (analysis_date - df["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df["order_num_total"]
rfm["monetary"] = df["customer_value_total"]                                 
                                 
rfm.head()
rfm.describe().T
rfm.shape



#GÖREV 3

rfm["recency_score"]=pd.qcut(rfm["recency"],5,labels=[5,4,3,2,1])
rfm["frequency_score"]=pd.qcut(rfm["frequency"].rank(method="first"),5,labels=[1,2,3,4,5])
rfm["monetary_score"]=pd.qcut(rfm["monetary"],5,labels=[1,2,3,4,5])

rfm["RF_SCORE"]=(rfm["recency_score"].astype(str)+
                 rfm["frequency_score"].astype(str))

rfm.describe().T
rfm.head()


#GÖREV 4

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}


rfm["segment"]=rfm["RF_SCORE"].replace(seg_map, regex=True)
rfm.head()

#GÖREV 5

rfm[["segment","recency","frequency","monetary"]].groupby("segment").agg(["mean","count"])

# # FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. 
# Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. 
# Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki 
# müşterilerle özel olarak iletişime geçmek isteniliyor. 
# Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden 
# alışveriş yapan kişiler özel olarak iletişim kurulacak müşteriler. 
# Bu müşterilerin id numaralarını csv dosyasına kaydediniz.


target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
cust_ids.shape

rfm.head()

# Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. 
# Bu indirimle ilgili kategorilerle ilgilenen geçmişte
# iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi 
# gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler 
# özel olarak hedef alınmak isteniyor. Uygun profildeki 
# müşterilerin id'lerini csv dosyasına kaydediniz.


target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)












