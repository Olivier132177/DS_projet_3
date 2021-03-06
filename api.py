from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
from elasticsearch import Elasticsearch
import numpy as np

es = Elasticsearch()
api = FastAPI()


def exec(req):
    """
    execute une requête SQL passée en paramètres"""

    resu = es.sql.query(body={"query": req, "columnar": True})
    dff = pd.DataFrame(resu["values"], index=[x["name"] for x in resu["columns"]]).T
    return dff


class Produit(BaseModel):
    uniq_id: Optional[int]
    product_name: Optional[str]
    manufacturer: Optional[str]
    price: Optional[float]
    number_of_reviews: Optional[int]
    number_of_answered_questions: Optional[int]
    average_review_rating: Optional[float]
    customers_who_bought_this_item_also_bought: Optional[str]
    description: Optional[str]
    product_information: Optional[str]
    product_description: Optional[str]
    items_customers_buy_after_viewing_this_item: Optional[str]
    customer_reviews: Optional[str]
    stock: Optional[int]
    type_stock: Optional[str]
    categorie_1: Optional[str]
    categorie_2: Optional[str]
    categorie_3: Optional[str]
    categorie_4: Optional[str]
    categorie_5: Optional[str]


@api.put("/ajoute_produit", tags=["Opérations de base"])
def ajoute_produit(prod: Produit):
    """
    ajoute un enregistrement dans la base produits"""

    doc = {
        "uniq_id": prod.uniq_id,
        "product_name": prod.product_name,
        "manufacturer": prod.manufacturer,
        "price": prod.price,
        "number_of_reviews": prod.number_of_reviews,
        "number_of_answered_questions": prod.number_of_answered_questions,
        "average_review_rating": prod.average_review_rating,
        "customers_who_bought_this_item_also_bought": prod.customers_who_bought_this_item_also_bought,
        "description": prod.description,
        "product_information": prod.product_information,
        "product_description": prod.product_description,
        "items_customers_buy_after_viewing_this_item": prod.items_customers_buy_after_viewing_this_item,
        "customer_reviews": prod.customer_reviews,
        "stock": prod.stock,
        "type_stock": prod.type_stock,
        "categorie_1": prod.categorie_1,
        "categorie_2": prod.categorie_2,
        "categorie_3": prod.categorie_3,
        "categorie_4": prod.categorie_4,
        "categorie_5": prod.categorie_5,
    }

    es.index(index="produits", document=doc)


@api.delete("/supprime_produit", tags=["Opérations de base"])
def supprime_produit(id_prod):
    """supprime le produit dont l'id est indiqué en paramètres"""
    es.delete(index="produits", id=id_prod)


@api.post("/update_produit", tags=["Opérations de base"])
def update_produit(prod: Produit, idprod: str):
    """met à jour le produit dont l'id est fourni avec les paramètres indiqués"""

    doc = {
        "uniq_id": prod.uniq_id,
        "product_name": prod.product_name,
        "manufacturer": prod.manufacturer,
        "price": prod.price,
        "number_of_reviews": prod.number_of_reviews,
        "number_of_answered_questions": prod.number_of_answered_questions,
        "average_review_rating": prod.average_review_rating,
        "customers_who_bought_this_item_also_bought": prod.customers_who_bought_this_item_also_bought,
        "description": prod.description,
        "product_information": prod.product_information,
        "product_description": prod.product_description,
        "items_customers_buy_after_viewing_this_item": prod.items_customers_buy_after_viewing_this_item,
        "customer_reviews": prod.customer_reviews,
        "stock": prod.stock,
        "type_stock": prod.type_stock,
        "categorie_1": prod.categorie_1,
        "categorie_2": prod.categorie_2,
        "categorie_3": prod.categorie_3,
        "categorie_4": prod.categorie_4,
        "categorie_5": prod.categorie_5,
    }
    doc = {x: doc[x] for x in doc.keys() if doc[x]}
    es.update(index="produits", id=idprod, doc=doc)


@api.get("/retourne_produit", tags=["Opérations de base"])
def retourne_produit(idprod: str):
    """retourne le detail du produit correspondant à l'id fourni"""
    es.get(index="produits", id=idprod)


@api.get("/produits_categories", tags=["Requetes spécifiques"])
def produits_categories():

    """retourne le nombre de produits par catégorie de premier niveau"""

    req = """SELECT count(*) as nombre, categorie_1 
                FROM produits 
                GROUP BY categorie_1 ORDER BY nombre DESC"""
    dff = exec(req)
    return dff.to_json()


@api.get("/produits_manufacturer/", tags=["Requetes spécifiques"])
def produits_manufacturer(nb: int = 10):
    """retourne les manufacturer et le nombre de produits associés par ordre descendant

    arguments:

        nb : nombre de produits retournés"""

    req = """SELECT manufacturer, count(*) as nb_produits, avg(price) as prix_moyen 
                FROM produits
                GROUP BY manufacturer
                ORDER BY nb_produits DESC
                LIMIT {}""".format(
        nb
    )
    dff = exec(req)
    return dff.to_json()


@api.get("/produits_prix/", tags=["Requetes spécifiques"])
def produits_prix(mini, maxi, nb=10):
    """retourne les produits, leur prix et leur stock pour une fourchette de prix donnée

    arguments:

        mini : prix minimal des produits
        maxi : prix maximal des produits
        nb : nombre de produits retournés

    """
    req = """SELECT product_name, round(price,1) as prix, stock 
                FROM produits 
                WHERE price BETWEEN {} AND {}
                ORDER BY price DESC LIMIT {} """.format(
        mini, maxi, nb
    )

    dff = exec(req)
    return dff.to_json()


@api.get("/ecart_prix_vendeur", tags=["Requetes spécifiques"])
def ecart_prix_vendeurs(ecart=0, nb=10):
    """retourne une liste de produits avec, pour chacun, les prix de vente minimum et maximum remarqués chez les revendeurs, ainsi que l'écart entre les 2

    arguments :

        ecart : écart minimal des produits à afficher dans la liste
        nb : nombre de produits retournés
    """
    req = """SELECT uniq_id, round(MIN(prix),1) as mini, round(MAX(prix),1) as maxi, round(MAX(prix) - MIN(prix),1) as ecart
                FROM vendeurs
                GROUP BY uniq_id
                HAVING ecart > {}
                """.format(
        ecart
    )
    dff = exec(req)
    return dff.head(nb).to_json()


@api.get("/mot_conversation", tags=["Requetes spécifiques"])
def mot_conversation(mot, nb=10):
    """retourne la liste des produits et les conversations associées dont les conversations contiennent une chaine de caractères donnée

    Arguments :

        mot : chaine de caractères que doivent contenir les conversations
        nb : nombre de produits retournés
    """

    req = """ SELECT uniq_id, conversation 
                FROM questions 
                WHERE conversation LIKE '%{}%'
                LIMIT 10
                """.format(
        mot, nb
    )

    dff = exec(req)
    return dff.to_json()


@api.get("/vendeurs_produits", tags=["Requetes spécifiques"])
def vendeurs_produits(id):
    """retourne le nom et le prix des revendeurs d'un produit donné

    Argument :

        id : id du produit
    """
    req = """SELECT *
                FROM vendeurs
                WHERE uniq_id = '{}'
                ORDER BY prix DESC
                """.format(
        id
    )
    dff = exec(req)
    return dff.to_json()


@api.get("/reviews_date", tags=["Requetes spécifiques"])
def reviews_date(date_comm):
    """retourne les notes et commentaires produits créés à une date donnée

    Arguments :

        date_comm : date au format YYYY-MM-DD
    """
    req = """ SELECT resume, note, date_clean, commentaire 
                FROM reviews
                WHERE date_clean ='{}'
             """.format(
        date_comm
    )

    dff = exec(req)
    return dff.to_json()
