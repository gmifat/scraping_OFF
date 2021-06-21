import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import process_time
from datetime import datetime
import re
import threading


# scraping Open Food Facts

# récupérer tous les produits de plusieurs pages
# entre start_page end_page
def get_all_products(start_page, end_page):
    start = process_time()
    products = []
    for page in range(start_page, end_page):
        page_products = get_products_from_page(page)
        products.extend(page_products)

    elapsed = (process_time() - start)
    print(F"Temps d' exécution de la méthode get_all_products est: {elapsed} {start_page} à {end_page}")
    return products


# récupérer les produit de page numéro page_index
def get_products_from_page(page_index):
    start = process_time()
    print("Début Traitement de la page " + str(page_index))
    products = []
    try:
        nb_pdt = 0
        # récupérer tous les liens relatifs des produits de cette page
        page_response = requests.get("https://fr.openfoodfacts.org/" + str(page_index))
        site_soup = BeautifulSoup(page_response.text, 'html.parser')
        product_relative_links = site_soup.find('div', attrs={'id': 'search_results'}).find_all('a')
        # On scrape le contenu de chaque produit
        for product_relative_link in product_relative_links:
            product = get_product("https://fr.openfoodfacts.org" + product_relative_link.attrs['href'])
            products.append(product)
            nb_pdt = nb_pdt + 1
            print(f'{page_index}.{nb_pdt}')
    except Exception as exp:
        print(f"--> Une exception get_products_from_page : {exp} à la page {page_index}")

    elapsed = (process_time() - start)
    print(f'Fin Traitement de la page {page_index} en {elapsed} secondes')
    return products


def get_product(product_link):
    start = process_time()
    print("Début Traitement du produit : " + product_link)
    product = {}
    try:
        product_response = requests.get(product_link)
        soup = BeautifulSoup(product_response.text.replace("\n", "").replace("\t", ""), 'html.parser')
        # main_tag = soup.find('div', attrs={'itemtype': 'https://schema.org/Product'})
        main_tag = soup.find('div', attrs={'id': 'main_column'})

        #  Informations du produit
        product.update(get_product_information(main_tag, product_link))

        # Caractéristiques du produit
        product_characteristics_tag = main_tag.find('h2', attrs={'id': 'product_characteristics'})
        if product_characteristics_tag is not None:
            product.update(get_product_characteristics(product_characteristics_tag.next_sibling, product_link))

        # Ingrédients
        product_ingredients_tag = main_tag.find('h2', attrs={'id': 'ingredients'})
        if product_ingredients_tag is not None:
            product.update(get_product_ingredients(product_ingredients_tag.next_sibling, product_link))

        # Informations nutritionnelles
        nutritional_information_tag = main_tag.find('h2', attrs={'id': 'nutrition_data'})
        if nutritional_information_tag is not None:
            product.update(get_nutritional_information(nutritional_information_tag.next_sibling.next_sibling,
                                                       product_link))

        # Comparaison avec les valeurs moyennes des produits de même catégorie :
        comparaison_information_tag = main_tag.find('table', attrs={'id': 'nutrition_data_table'})
        if comparaison_information_tag is not None:
            product.update(get_percent_difference(comparaison_information_tag, product_link))

        # Impact environnemental
        environmental_impact_tag = main_tag.find('table', attrs={'id': 'agribalyse_impacts'})
        if environmental_impact_tag is not None:
            product.update(get_environmental_impact(environmental_impact_tag, product_link))
    except Exception as exp:
        print(f"--> get_product exception : {product_link} {exp}")

    print("Fin Traitement du produit  : " + product_link)
    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_product est: ", elapsed)
    return product


# récupérer les information d' un produit
def get_product_information(main_tag, product_link):
    start = process_time()
    product_information = {}
    try:
        tag_name = main_tag.find('h1', attrs={'property': 'food:name'})
        if tag_name is not None:
            product_information['name'] = tag_name.text.strip().replace('\xa0', '')

        tag_bar_code = main_tag.find('span', attrs={'id': 'barcode'})
        if tag_bar_code is not None:
            product_information['bar_code'] = tag_bar_code.text

        tag_eco_score = main_tag.find_all('a', attrs={'href': '/ecoscore'})
        if tag_eco_score is not None and len(tag_eco_score) > 2:
            product_information['eco_score'] = tag_eco_score[2].find('img').attrs['alt'].split(' ')[1]

    except Exception as exp:
        print(f"--> get_product_information exception {product_link} {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_product_information est: ", elapsed)
    return product_information


def get_product_characteristics(product_characteristics_tag, product_link):
    start = process_time()
    product_characteristics = {}
    try:
        tag_generic_name = product_characteristics_tag.find('span', attrs={'itemprop': 'description'})
        if tag_generic_name is not None:
            product_characteristics["generic_name"] = tag_generic_name.text.strip().replace('\xa0', '')

        tag_fields = product_characteristics_tag.find_all('span', attrs={'class': 'field'})
        for tag_field in tag_fields:
            name = get_column_name(tag_field.text.replace('\xa0:', '').strip(), product_link)
            tag_a = tag_field.parent.find_all('a')
            if len(tag_a) == 0:
                product_characteristics[name] = tag_field.next_sibling.strip()
            elif name == 'link':
                product_characteristics[name] = tag_a[0].attrs['href']
            else:
                product_characteristics[name] = get_list_of_items(tag_a)

    except Exception as exp:
        print(f"--> get_product_characteristics exception : {product_link} {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_product_characteristics est: ", elapsed)
    return product_characteristics


# Extraire le contenu de chaque élément d' une liste de balise pour créer une liste de string
def get_list_of_items(tag_items):
    items_data = []
    try:
        for tag_item in tag_items:
            items_data.append(tag_item.text.strip())
    except Exception as exp:
        print("--> get_list_of_items exception :", exp)

    return items_data


def get_product_ingredients(product_ingredients_tag, product_link):
    start = process_time()
    product_ingredients = {}
    try:
        ingredients_list = product_ingredients_tag.find('div', attrs={'id': 'ingredients_list'})
        if ingredients_list is not None:
            product_ingredients["ingredients_list"] = list(filter(lambda s: s, re.split(', |\.|\(|\)|;',
                                                                                        ingredients_list.text.strip())))

        additives = product_ingredients_tag.find('b', text='Additifs\xa0:')
        if additives is not None:
            product_ingredients["additives"] = get_list_of_items(additives.parent.find('ul').find_all('a'))

        palm_ingredients = product_ingredients_tag.find('b', text="Ingrédients issus de l\'huile de palme\xa0:")
        if palm_ingredients is not None:
            product_ingredients["palm_ingredients"] = get_list_of_items(
                palm_ingredients.parent.find('ul').find_all('a'))

        allergens = product_ingredients_tag.find('span',
                                                 attrs={'class': 'field'},
                                                 text='Substances ou produits provoquant '
                                                      'des allergies ou intolérances\xa0:')
        if allergens is not None:
            product_ingredients["allergens"] = get_list_of_items(allergens.parent.find_all('a'))

        ingredients_analysis = product_ingredients_tag.find_all('span', attrs={'class': 'ingredients_analysis'})
        product_ingredients["ingredients_analysis"] = get_list_of_items(ingredients_analysis)

        tag_nova = product_ingredients_tag.find_all('a', attrs={'href': '/nova'})
        if len(tag_nova) > 1:
            nova_data = tag_nova[1].find('img').attrs['alt'].split(' - ')
            product_ingredients["nova_score"] = nova_data[0]

    except Exception as exp:
        print(f"--> get_product_ingredients exception {product_link} : {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_product_ingredients est: ", elapsed)
    return product_ingredients


def get_nutritional_information(nutritional_information_tag, product_link):
    start = process_time()
    product_nutritional_information = {}
    try:

        fats_lipids = nutritional_information_tag.find('b', text='Matières grasses / Lipides')
        if fats_lipids is not None:
            product_nutritional_information["fats_lipids_quantity"] = fats_lipids.previous_sibling.strip()
            product_nutritional_information["fats_lipids_comment"] = fats_lipids.next_sibling.strip()

        saturated_acids = nutritional_information_tag.find('b', text='Acides gras saturés')
        if saturated_acids is not None:
            product_nutritional_information["saturated_acids_quantity"] = saturated_acids.previous_sibling.strip()
            product_nutritional_information["saturated_acids_comment"] = saturated_acids.next_sibling.strip()

        sugar = nutritional_information_tag.find('b', text='Sucres')
        if sugar is not None:
            product_nutritional_information["sugar_quantity"] = sugar.previous_sibling.strip()
            product_nutritional_information["sugar_comment"] = sugar.next_sibling.strip()

        salt = nutritional_information_tag.find('b', text='Sel')
        if salt is not None:
            product_nutritional_information["salt_quantity"] = salt.previous_sibling.strip()
            product_nutritional_information["salt_comment"] = salt.next_sibling.strip()

    except Exception as exp:
        print(f"--> get_product_nutritional_information exception {product_link} : {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_nutritional_information est: ", elapsed)
    return product_nutritional_information


def get_percent_difference(comparaison_information_tag, product_link):
    start = process_time()
    product_comparaison_information = {}
    try:
        # Énergie (kJ)
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_energy-kj_tr', 'kj'))

        # Énergie (kcal)
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_energy-kcal_tr', 'kcal'))
        # Énergie
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_energy_tr'))

        # Matières grasses / Lipides
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_fat_tr'))

        # Acides gras saturés
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_saturated-fat_tr'))

        # Glucides
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_carbohydrates_tr'))

        # Sucres
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_sugars_tr'))

        # Protéines
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_proteins_tr'))

        # Sel
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_salt_tr'))

        # Sodium
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_sodium_tr'))

        #
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_nutrition-score-fr_tr'))

        # Nutri-Score
        product_comparaison_information.update(
            get_item_from_table(comparaison_information_tag, 'nutriment_nutriscore_tr'))

    except Exception as exp:
        print(f"get_percent_difference exception {product_link} : {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_percent_difference est: ", elapsed)
    return product_comparaison_information


# Lire le contenu d' une ligne de la table Informations nutritionnelles
# et retourné un dictionnaire contenant l' id de la ligne, la valeur et la comparaison
def get_item_from_table(tag_table, att_id, unit='g'):
    values = {}
    try:
        tag_tr = tag_table.find('tr', attrs={'id': att_id})
        if tag_tr is not None:
            tag_td = tag_tr.find('td', attrs={'class': 'nutriment_value'})

            if tag_td is not None:
                values[att_id] = tag_td.text.replace('\xa0', '')\
                    .replace(unit, '').strip()

            tag_td = tag_tr.find('td', attrs={'class': 'nutriment_value compare_0'})
            if tag_td is not None:
                # Comparaison avec la valeur moyenne
                values[att_id + '_diff'] = tag_td.contents[0].text.replace('\xa0', '').strip()

    except Exception as exp:
        print("get_item_from_table exception :", exp)

    return values


def get_environmental_impact(environmental_impact_tag, product_link):
    start = process_time()
    product_environmental_impact = {}
    try:
        impact_list = environmental_impact_tag.find_all('tr')
        for impact_index in range(1, len(impact_list)):
            tags = impact_list[impact_index].find_all('td')
            product_environmental_impact[tags[0].text.strip()] = tags[1].text.strip()

        # Score ACV sur 100
        product_environmental_impact["Score_ACV"] = environmental_impact_tag.next_sibling.text.split(':')[1].strip()

    except Exception as exp:
        print(f"--> get_environmental_impact exception {product_link} : {exp}")

    elapsed = (process_time() - start)
    print("Temps d' exécution de la méthode get_environmental_impact est: ", elapsed)
    return product_environmental_impact


def get_column_name(label, product_link):
    if label == "Dénomination générique":
        return "generic_name"

    if label == "Quantité":
        return "quantity"

    if label == "Conditionnement":
        return "packaging"

    if label == "Marques":
        return "brand"

    if label == "Catégories":
        return "categories"
    #
    if label == "Labels, certifications, récompenses":
        return "labels"
    #
    if label == "Origine des ingrédients":
        return "ingredients_origins"
    #
    if label == "Lieux de fabrication ou de transformation":
        return "manufacturing"
    #
    if label == "Code de traçabilité":
        return "traceability_code"
    #
    if label == "Lien vers la page du produit sur le site officiel du fabricant":
        return "link"
    #
    if label == "Magasins":
        return "markets"
    #
    if label == "Pays de vente":
        return "sales_countries"

    if label == "Producteur":
        return "producer_info"

    if label == "Origine":
        return "origin_detail"

    if label == "Propriétaire de la marque":
        return "brand_owner"

    print(f"label inconnue : {label} {product_link}")
    return label


# utilisation de multithreading
# https://www.tutorialspoint.com/python/python_multithreading.htm
class GeProductThread (threading.Thread):
    def __init__(self, thread_id, name, start_index, end_index):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.name = name
        self.start_index = start_index
        self.end_index = end_index

    def run(self):
        start_time_th = datetime.now()
        print(f"start thread {self.name} {self.start_index} --> {self.end_index} {start_time_th}")
        my_products = get_all_products(self.start_index, self.end_index)
        if len(my_products) > 0:
            my_df = pd.DataFrame.from_records(my_products)
            my_df.to_csv(f"/Users/gmidenfatma/Documents/dev/python/scraping_OFF/files_csv/output_{self.threadID}.csv")
        print(f"end thread {self.name} {self.start_index} --> {self.end_index} "
              f"{(datetime.now() - start_time_th).total_seconds()}")


start_process_time = process_time()
start_time = datetime.now()

ctr = 1
count = 20
nb_iter = 52
nb_threads = 8
for i in range(1, nb_iter):
    threads = []
    for th in range(0, nb_threads):
        threads.append(GeProductThread((nb_iter * th) + i, f"Thread-{i}.{th}", ctr, ctr+count))
        ctr = ctr + count

    # # Start new Threads
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print(f'fin itération {i}')


end_time = datetime.now()
print(f'{start_time} --> {end_time} = {(end_time - start_time).total_seconds()}')
elapsed_all = (process_time() - start_process_time)
print(F"Temps d' exécution de la méthode get_all_products est: {elapsed_all}")
