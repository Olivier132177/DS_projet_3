from dataclasses import fields
from datetime import date
from distutils.log import error
from elasticsearch import Elasticsearch
import pandas as pd
import re
import numpy as np


def loading(fichier, index):
    """prends en entrée un dataframe et le nom souhaité de l'index pour
    alimenter une table ElasticSearch via l'api bulk"""
    tab_bulk = []
    for i, val in fichier.iterrows():
        tab_bulk.append({"create": {"_index": index, "_id": i}})
        tab_bulk.append(val.dropna().to_dict())
    resp = es.bulk(body=tab_bulk)
    return resp


def verif_price(val):
    """vérifie si une valeur a un format de prix valide. Si ce n'est pas le cas retourne np.nan"""
    regex = re.compile("^\d*[.]\d+$")
    if not regex.match(val):
        val = np.nan
    return val


def verif_note(val):
    """vérifie si une valeur a un format de note valide (float entre 0 et 5). Si ce n'est pas le cas retourne np.nan"""
    regex = re.compile("^[12345][.]0$")
    if not regex.match(val):
        return np.nan
    else:
        return val


def retrait_price(ser):

    """convertit le champ prix en float exploitable"""
    ser = ser.str.strip("£")
    ser = ser.map(lambda x: verif_price(str(x)))
    ser = ser.astype("float")
    return ser


def retrait_stock(ser):
    """divise en 2 le champs stock : 1 champ avec le type, 1 champ avec la valeur"""

    stock = ser.str.split(expand=True)[0].astype("float").fillna(0)
    type_stock = ser.str.split(expand=True)[1].fillna("no")

    return stock, type_stock


def retrait_review_rating(ser):
    """extrait la note donnée de la chaine de caractères"""
    ser = ser.str.split(expand=True)[0].astype("float")
    return ser


def retrait_number_reviews(ser):
    """convertit le nombre de reviews, initialement en chaine de caractères, en float"""
    ser = ser.str.replace(",", "").astype(float)
    return ser


def retrait_categories(ser):
    """extrait toutes les sous-catégories de la chaine de caractère et les retourne en colonnes distinctes"""
    ser_split = ser.str.split(">", expand=True)
    return ser_split[0], ser_split[1], ser_split[2], ser_split[3], ser_split[4]


def conv_date(ser):
    """tente une conversion en format date. En cas d'échecs retourne np.nan"""
    try:
        result = date(int(ser[2]), int(ser[1]), int(ser[0]))
    except:
        result = np.nan
    return result


def clean_df(df_raw):

    """prend en entrée la version brute de la base et retourne une version nettoyée
    dont les champs représentant des listes sont splités en  plusieurs colonnes"""

    df = df_raw.copy()
    df.loc[:, "price"] = retrait_price(df.loc[:, "price"])
    df.loc[:, "stock"], df.loc[:, "type_stock"] = retrait_stock(
        df.loc[:, "number_available_in_stock"]
    )
    df.loc[:, "average_review_rating"] = retrait_review_rating(
        df.loc[:, "average_review_rating"]
    )
    df.loc[:, "number_of_reviews"] = retrait_number_reviews(
        df.loc[:, "number_of_reviews"]
    )
    (
        df.loc[:, "categorie_1"],
        df.loc[:, "categorie_2"],
        df.loc[:, "categorie_3"],
        df.loc[:, "categorie_4"],
        df.loc[:, "categorie_5"],
    ) = retrait_categories(df.loc[:, "amazon_category_and_sub_category"])

    return df


def create_df_reviews(df_reviews):

    """extrait les reviews de la variable customer_reviews pour créér un dataframe avec une ligne par review"""

    df_reviews = df_reviews.loc[:, ["uniq_id", "customer_reviews"]]
    rev = df_reviews["customer_reviews"].str.split("|", expand=True)
    df_reviews = df_reviews.join(rev).drop("customer_reviews", axis=1)
    df_reviews = (
        pd.melt(df_reviews, id_vars="uniq_id")
        .rename({"variable": "num_review", "value": "review"}, axis=1)
        .dropna()
    )
    df_detail = df_reviews.copy()
    df_detail_split = df_detail["review"].str.split("//", expand=True)
    df_detail_split.columns = [
        "resume",
        "note",
        "date",
        "qui",
        "commentaire",
        "autre_1",
        "autre_2",
        "autre_3",
        "autre_4",
    ]
    df_details_final = df_detail.join(df_detail_split).drop("review", axis=1)

    df_details_final.loc[:, "note"] = (
        df_details_final.loc[:, "note"]
        .apply(lambda x: str(x).strip())
        .apply(lambda x: verif_note(str(x)))
        .astype("float")
    )

    df_date = df_details_final["date"].str.split(expand=True)
    df_date[0] = df_date[0].astype("float")
    df_date[2] = df_date[2].astype("float")
    map_mois = {
        "April": 4,
        "Jun.": 6,
        "Dec.": 12,
        "Mar.": 3,
        "Oct.": 10,
        "Jan.": 1,
        "May": 5,
        "Aug.": 8,
        "July": 7,
        "Nov.": 11,
        "Feb.": 2,
        "Sept.": 9,
    }
    df_date[1] = df_date[1].map(map_mois)

    df_date["date_clean"] = df_date.apply(conv_date, axis=1)
    df_date = df_date.drop([0, 1, 2], axis=1)
    df_details_final = df_details_final.join(df_date).drop("date", axis=1)
    df_details_final["date_clean"] = pd.to_datetime(df_details_final["date_clean"])

    return df_details_final


def create_df_questions(df_questions):

    """extrait les reviews de la variable customer_reviews pour créér un dataframe avec une ligne par review"""

    df_questions = raw_df.copy()
    df_questions = df_questions.loc[
        :, ["uniq_id", "customer_questions_and_answers"]
    ].dropna()
    det_ques = df_questions["customer_questions_and_answers"].str.split(
        "|", expand=True
    )
    df_questions = df_questions.drop("customer_questions_and_answers", axis=1).join(
        det_ques
    )
    df_questions = (
        pd.melt(df_questions, id_vars="uniq_id")
        .rename({"variable": "num_echange", "value": "conversation"}, axis=1)
        .dropna()
    )
    return df_questions


def create_df_sellers(df):

    """extrait les revendeurs et les prix de la variable sellers pour créer un dataframe avec une ligne par revendeur par produit"""

    ser = df["sellers"].dropna()
    ser = ser.map(lambda x: x.replace('{"seller"=>', ""))
    ser = ser.str.split("},").explode()
    ser = ser.str.split('",', expand=True)
    ser.loc[:, 0] = ser.loc[:, 0].apply(lambda x: x.split("=>")[1]).str.strip('}{][" ')
    ser.loc[:, 1] = (
        ser.loc[:, 1]
        .apply(lambda x: x.split("=>")[1])
        .str.strip('}{]["£ ')
        .str.replace(",", "")
        .astype("float")
    )
    ser.columns = ["revendeur", "prix"]

    df_ventes = ser.join(df["uniq_id"])
    df_ventes = df_ventes.reset_index().drop("index", axis=1)
    return df_ventes


def create_df_produits(df_produits):

    """supprime les colonnes non utilisées du dataframe initial pour créer la table produits"""

    df_produits = df.drop(
        columns=[
            "amazon_category_and_sub_category",
            "number_available_in_stock",
            "customer_questions_and_answers",
            "sellers",
        ]
    )
    return df_produits


###########################################################################

es = Elasticsearch()

raw_df = pd.read_csv("amazon_co-ecommerce_sample.csv")

df = clean_df(raw_df)
df_produits = create_df_produits(df)
df_reviews = create_df_reviews(df)
df_questions = create_df_questions(df)
df_vendeurs = create_df_sellers(df)

loading(fichier=df_produits.iloc[:5000], index="produits")
print("ok produits part 1")
loading(fichier=df_produits.iloc[5000:], index="produits")
print("ok produits part 2")
loading(fichier=df_reviews.iloc[:15000], index="reviews")
print("ok reviews part 1")
loading(fichier=df_reviews.iloc[15000:], index="reviews")
print("ok reviews part 2")
loading(fichier=df_questions, index="questions")
print("ok questions")
loading(fichier=df_vendeurs.iloc[:16000], index="vendeurs")
print("ok vendeurs 1")
loading(fichier=df_vendeurs.iloc[16000:], index="vendeurs")
print("ok vendeurs 2")

df_questions.columns
