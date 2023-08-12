import pymonetdb
import pandas as pd
import time
import matplotlib.pyplot as plt

def connexion():
    return pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="demo")

def creation_dataframe():
    return pd.read_csv('base-sirene-v3-consolidee-sur-l-eurometropole-de-strasbourg.csv', sep=';', dtype={'complementadresse2etablissement': 'str', 'indicerepetition2etablissement': 'str', 'enseigne2etablissement': 'str', 'enseigne3etablissement': 'str', 'unitepurgeeunitelegale': 'str', 'denominationusuelle3unitelegale': 'str'})

def exec_fichier(cursor, fichier):
    fd = open(fichier, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            if command.strip() != '':
                cursor.execute(command)
        except (IOError, msg):
            print("Command skipped: ", msg)

def remplissage_tables_non_fragmentees(array, cursor, data, limit):
    current_row = 1

    for index, row in data.iterrows():
        requete = "INSERT INTO TABLENONFRAGMENTEE VALUES (" + str(current_row) + ', '

        for i in range(len(row)):
            if i==0:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += str(row[i])+', '
            elif i>0 and i<3:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>2 and i<49:
                if(i==9):
                    if(str(row[i]) == "nan"):
                        requete += 'NULL, '
                    else:
                        splitted = str(row[i]).split('+')
                        splitted = splitted[0].split('T')
                        requete += 'str_to_timestamp(\'' + splitted[0] + ' ' + splitted[1] +'\', \'%Y-%m-%d %H:%M:%s\'), '
                elif(i==4 or i==6 or i==11 or i==13 or i==24 or i==27 or i==40):
                    if(i==4 or i==40):
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete += 'NULL, '
                    else:
                        requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>48 and i<66:
                if(i==64):
                    if(str(row[i]) == "nan"):
                        requete += 'NULL, '
                    else:
                        splitted = str(row[i]).split('+')
                        splitted = splitted[0].split('T')
                        requete += 'str_to_timestamp(\'' + splitted[0] + ' ' + splitted[1] +'\', \'%Y-%m-%d %H:%M:%s\'), '
                elif(i==51 or i==62 or i==65):
                    if(i==51):
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete += 'NULL, '
                    else:
                        requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==66:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==67:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_') + '\', '
            elif i>67 and i<83:
                if(i==68):
                    if(i==68):
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete += 'NULL, '
                        else:
                            requete += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete += 'NULL, '
                    else:
                        requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>82 and i<95:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>94 and i<102:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==102:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==103:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==104:
                if(str(row[i]) == "nan"):
                    requete += 'NULL, '
                else:
                    requete += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
            elif i==105:
                if(str(row[i]) == "nan"):
                    requete += 'NULL'
                else:
                    requete += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\')'

        requete += ')'

        save_performance(array, cursor, requete)

        current_row+=1
        if current_row > limit:
            break

def remplissage_tables_fragmentees(array, cursor, data, limit):
    current_row = 1

    for index, row in data.iterrows():
        requete_etablissement = "INSERT INTO ETABLISSEMENT VALUES (" + str(current_row) + ', '
        requete_unitelegale = "INSERT INTO UNITELEGALE VALUES (" + str(current_row) + ', '
        requete_entreprise = "INSERT INTO ENTREPRISE VALUES (" + str(current_row) + ', '

        for i in range(len(row)):
            if i==0:
                if(str(row[i]) == "nan"):
                    requete_entreprise += 'NULL, '
                else:
                    requete_entreprise += str(row[i])+', '
            elif i>0 and i<3:
                if(str(row[i]) == "nan"):
                    requete_entreprise += 'NULL, '
                else:
                    requete_entreprise += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>2 and i<49:
                if(i==9):
                    if(str(row[i]) == "nan"):
                        requete_etablissement += 'NULL, '
                    else:
                        splitted = str(row[i]).split('+')
                        splitted = splitted[0].split('T')
                        requete_etablissement += 'str_to_timestamp(\'' + splitted[0] + ' ' + splitted[1] +'\', \'%Y-%m-%d %H:%M:%s\'), '
                elif(i==4 or i==6 or i==11 or i==13 or i==24 or i==27 or i==40):
                    if(i==4 or i==40):
                        if(str(row[i]) == "nan"):
                            requete_etablissement += 'NULL, '
                        else:
                            requete_etablissement += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete_etablissement += 'NULL, '
                        else:
                            requete_etablissement += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete_etablissement += 'NULL, '
                    else:
                        requete_etablissement += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>48 and i<66:
                if(i==64):
                    if(str(row[i]) == "nan"):
                        requete_unitelegale += 'NULL, '
                    else:
                        splitted = str(row[i]).split('+')
                        splitted = splitted[0].split('T')
                        requete_unitelegale += 'str_to_timestamp(\'' + splitted[0] + ' ' + splitted[1] +'\', \'%Y-%m-%d %H:%M:%s\'), '
                elif(i==51 or i==62 or i==65):
                    if(i==51):
                        if(str(row[i]) == "nan"):
                            requete_unitelegale += 'NULL, '
                        else:
                            requete_unitelegale += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete_unitelegale += 'NULL, '
                        else:
                            requete_unitelegale += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete_unitelegale += 'NULL, '
                    else:
                        requete_unitelegale += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==66:
                if(str(row[i]) == "nan"):
                    requete_entreprise += 'NULL, '
                else:
                    requete_entreprise += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==67:
                if(str(row[i]) == "nan"):
                    requete_entreprise += 'NULL'
                else:
                    requete_entreprise += '\'' + str(row[i]).replace('\'', '_') + '\''
            elif i>67 and i<83:
                if(i==68):
                    if(i==68):
                        if(str(row[i]) == "nan"):
                            requete_unitelegale += 'NULL, '
                        else:
                            requete_unitelegale += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\'), '
                    else:
                        if(str(row[i]) == "nan"):
                            requete_unitelegale += 'NULL, '
                        else:
                            requete_unitelegale += str(row[i])+', '
                else:
                    if(str(row[i]) == "nan"):
                        requete_unitelegale += 'NULL, '
                    else:
                        requete_unitelegale += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>82 and i<95:
                if(str(row[i]) == "nan"):
                    requete_etablissement += 'NULL, '
                else:
                    requete_etablissement += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i>94 and i<102:
                if(str(row[i]) == "nan"):
                    requete_unitelegale += 'NULL, '
                else:
                    requete_unitelegale += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==102:
                if(str(row[i]) == "nan"):
                    requete_etablissement += 'NULL, '
                else:
                    requete_etablissement += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==103:
                if(str(row[i]) == "nan"):
                    requete_unitelegale += 'NULL, '
                else:
                    requete_unitelegale += '\'' + str(row[i]).replace('\'', '_')+'\', '
            elif i==104:
                if(str(row[i]) == "nan"):
                    requete_etablissement += 'NULL'
                else:
                    requete_etablissement += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\')'
            elif i==105:
                if(str(row[i]) == "nan"):
                    requete_unitelegale += 'NULL'
                else:
                    requete_unitelegale += 'str_to_date(\'' + str(row[i]).replace('/', '-') + '\', \'%d-%m-%Y\')'

        requete_etablissement += ')'
        requete_unitelegale += ')'
        requete_entreprise += ')'

        save_performance(array, cursor, requete_etablissement)
        save_performance(array, cursor, requete_unitelegale)
        save_performance(array, cursor, requete_entreprise)

        current_row+=1
        if current_row > limit:
            break

def save_performance(array, cursor, requete):
    time_debut = time.perf_counter()
    cursor.execute(requete)
    time_fin = time.perf_counter()
    array.append(time_fin-time_debut)

def afficher_graphique(array, xlabel, ylabel, title):
    plt.plot([i for i in range(1, len(array)+1)], array)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.show()

def plot_graphiques_insert():
    afficher_graphique(array = tempsInsertionsTablesNonFragmentees, xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (MonetDB, table non fragmentée)')
    afficher_graphique(array = tempsInsertionsTablesFragmenteesEnsembles, xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (MonetDB, tables fragmentées ensembles)')
    afficher_graphique(array = tempsInsertionsTableFragmenteeEtablissement, xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (MonetDB, table fragmentée ETABLISSEMENT)')
    afficher_graphique(array = tempsInsertionsTableFragmenteeUniteLegale, xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (MonetDB, table fragmentée UNITELEGALE)')
    afficher_graphique(array = tempsInsertionsTableFragmenteeEntreprise, xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (MonetDB, table fragmentée ENTREPRISE)')

db = connexion()
cursor = db.cursor()

exec_fichier(cursor, 'creation_tables_monetdb.sql')

data = creation_dataframe()

tempsInsertionsTablesNonFragmentees = []
tempsInsertionsTablesFragmenteesIndividuelles = []
tempsInsertionsTablesFragmenteesEnsembles = []
tempsInsertionsTableFragmenteeEtablissement = []
tempsInsertionsTableFragmenteeUniteLegale = []
tempsInsertionsTableFragmenteeEntreprise = []

limit = 100000

remplissage_tables_non_fragmentees(array = tempsInsertionsTablesNonFragmentees, cursor = cursor, data = data, limit = limit)

remplissage_tables_fragmentees(array = tempsInsertionsTablesFragmenteesIndividuelles, cursor = cursor, data = data, limit = limit)

for i in range(0, len(tempsInsertionsTablesFragmenteesIndividuelles), 3):
    value1 = tempsInsertionsTablesFragmenteesIndividuelles[i]
    value2 = tempsInsertionsTablesFragmenteesIndividuelles[i+1]
    value3 = tempsInsertionsTablesFragmenteesIndividuelles[i+2]
    tempsInsertionsTablesFragmenteesEnsembles.append(value1+value2+value3)

    tempsInsertionsTableFragmenteeEtablissement.append(value1)
    tempsInsertionsTableFragmenteeUniteLegale.append(value2)
    tempsInsertionsTableFragmenteeEntreprise.append(value3)


