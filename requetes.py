from dataclasses import fields
from distutils.log import error
from elasticsearch import Elasticsearch

import pandas as pd


def exec(req):
    resu = es.sql.query(body={"query": req, "columnar": True})
    dff = pd.DataFrame(resu["values"], index=[x["name"] for x in resu["columns"]]).T
    return dff

es = Elasticsearch()

"""
PRODUITS
       'uniq_id', 'product_name', 'manufacturer', 'price', 'number_of_reviews',
       'number_of_answered_questions', 'average_review_rating', 'description',
       'product_information', 'product_description',
       'items_customers_buy_after_viewing_this_item', 'customer_reviews',
       'stock', 'type_stock', 'categorie_1', 'categorie_2',
       'categorie_3', 'categorie_4', 'categorie_5'

REVIEWS
       'uniq_id', 'num_review', 'resume', 'note', 'qui', 'commentaire',
       'autre_1', 'autre_2', 'autre_3', 'autre_4', 'date_clean'

CONVERSATIONS
        'uniq_id', 'num_echange', 'conversation']

VENTES
        'revendeur', 'prix', 'uniq_id'
"""


def produits_categories():

    """retourne le nombre de produits par catégorie de premier niveau"""

    req = """SELECT count(*) as nombre, categorie_1 
                FROM produits 
                GROUP BY categorie_1 ORDER BY nombre DESC"""
    dff = execjs(req)
    return dff


def produits_manufacturer(nb=10):
    """retourne les manufacturer et le nombre de produits associés par ordre descendant

    argument :
            nb : nombre de produits retournés"""

    req = """SELECT manufacturer, count(*) as nb_produits, avg(price) as prix_moyen 
                FROM produits
                GROUP BY manufacturer
                ORDER BY nb_produits DESC
                LIMIT {}""".format(
        nb
    )
    dff = exec(req)
    return dff


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
    return dff


def ecart_prix_vendeurs(ecart, nb):
    """retourne une liste de produits avec, pour chacun, les prix de vente minimum et maximum remarqués chez les revendeurs, ainsi que l'écart entre les 2
    Arguments :
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
    return dff.head(nb)


def mot_conversation(mot, nb=10):
    """retourne la liste des produits et les conversations associées dont les conversations contiennent une chaine de caractères donnée
    arguments :
        mot : chaine de caractères que doivent contenir les conversations
        nb : nombre de produits retournés
    """

    req = """ SELECT uniq_id, conversation 
                FROM conversations 
                WHERE conversation LIKE '%{}%'
                LIMIT 10
                """.format(
        mot, nb
    )

    dff = exec(req)
    return dff


def vendeurs_produits(id):
    """retourne le nom et le prix des revendeurs d'un produit donné
    argument :
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
    return dff


def reviews_date(date_comm):
    """retourne les notes et commentaires produits créés à une date donnée
    arguments :
        date_comm : date au format YYYY-MM-DD
    """
    req = """ SELECT resume, note, date_clean, commentaire 
                FROM reviews
                WHERE date_clean ='2015-02-05'
             """.format(
        date_comm
    )

    dff = exec(req)
    return dff


produits_categories()
produits_manufacturer(nb=5)
produits_prix(mini=40, maxi=50, nb=5)
ecart_prix_vendeurs(ecart=100, nb=5)
mot_conversation(mot="wonderful", nb=5)
vendeurs_produits(id="0016eb63fa6c7a5e8930bc7732b13116")
reviews_date(date_comm="2015-02-05")
