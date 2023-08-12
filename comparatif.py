import testsMysql
import testsMonetdb
import time
import matplotlib.pyplot as plt

def afficher_graphique(array1, array2, array1label, array2label, xlabel, ylabel, title):
    plt.plot([i for i in range(1, len(array1)+1)], array1, label = array1label)
    plt.plot([i for i in range(1, len(array2)+1)], array2, label = array2label)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.show()
    pass

def save_performance(array, cursor, requete):
    time_debut = time.perf_counter()
    cursor.execute(requete)
    time_fin = time.perf_counter()
    array.append(time_fin-time_debut)

def plot_graphiques_insert():
    afficher_graphique(array1 = testsMysql.tempsInsertionsTablesNonFragmentees, array2 = testsMonetdb.tempsInsertionsTablesNonFragmentees, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (table non fragmentée)')
    afficher_graphique(array1 = testsMysql.tempsInsertionsTablesFragmenteesEnsembles, array2 = testsMonetdb.tempsInsertionsTablesFragmenteesEnsembles, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (tables fragmentées ensembles)')
    afficher_graphique(array1 = testsMysql.tempsInsertionsTableFragmenteeEtablissement, array2 = testsMonetdb.tempsInsertionsTableFragmenteeEtablissement, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (table fragmentée ETABLISSEMENT)')
    afficher_graphique(array1 = testsMysql.tempsInsertionsTableFragmenteeUniteLegale, array2 = testsMonetdb.tempsInsertionsTableFragmenteeUniteLegale, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (table fragmentée UNITELEGALE)')
    afficher_graphique(array1 = testsMysql.tempsInsertionsTableFragmenteeEntreprise, array2 = testsMonetdb.tempsInsertionsTableFragmenteeEntreprise, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'numéro de l\'insertion', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque insertion (table fragmentée ENTREPRISE)')

def plot_graphiques_select():
    timeSelectsMysql = []
    timeSelectsEtablissementMysql = []
    timeSelectsUniteLegaleMysql = []
    timeSelectsEntrepriseMysql = []
    timeSelectsMonetdb = []
    timeSelectsEtablissementMonetdb = []
    timeSelectsUniteLegaleMonetdb = []
    timeSelectsEntrepriseMonetdb = []
    requete = "SELECT "
    requete_etablissement = "SELECT "
    requete_unitelegale = "SELECT "
    requete_entreprise = "SELECT "
    for i in range(len(testsMysql.data.columns)-1):
        colonne = testsMysql.data.columns[i]
        requete += colonne
        save_performance(array = timeSelectsMysql, cursor = testsMysql.cursor, requete = requete+" FROM TABLENONFRAGMENTEE")
        save_performance(array = timeSelectsMonetdb, cursor = testsMonetdb.cursor, requete = requete+" FROM TABLENONFRAGMENTEE")
        requete += ', '

        if((i>2 and i<49) or (i>82 and i<95) or i==102 or i==104):
            requete_etablissement += colonne
            save_performance(array = timeSelectsEtablissementMysql, cursor = testsMysql.cursor, requete = requete_etablissement+" FROM ETABLISSEMENT")
            save_performance(array = timeSelectsEtablissementMonetdb, cursor = testsMonetdb.cursor, requete = requete_etablissement+" FROM ETABLISSEMENT")
            requete_etablissement += ', '
        if((i>48 and i<66) or (i>67 and i<83) or (i>94 and i<102) or i==103 or i==105):
            requete_unitelegale += colonne
            save_performance(array = timeSelectsUniteLegaleMysql, cursor = testsMysql.cursor, requete = requete_unitelegale+" FROM UNITELEGALE")
            save_performance(array = timeSelectsUniteLegaleMonetdb, cursor = testsMonetdb.cursor, requete = requete_unitelegale+" FROM UNITELEGALE")
            requete_unitelegale += ', '
        if((i>=0 and i<3) or (i>65 and i<68)):
            requete_entreprise += colonne
            save_performance(array = timeSelectsEntrepriseMysql, cursor = testsMysql.cursor, requete = requete_entreprise+" FROM ENTREPRISE")
            save_performance(array = timeSelectsEntrepriseMonetdb, cursor = testsMonetdb.cursor, requete = requete_entreprise+" FROM ENTREPRISE")
            requete_entreprise += ', '

    afficher_graphique(array1 = timeSelectsMysql, array2 = timeSelectsMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'nombre de colonnes sélectionnées', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque sélection (table non fragmentée)')
    afficher_graphique(array1 = timeSelectsEtablissementMysql, array2 = timeSelectsEtablissementMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'nombre de colonnes sélectionnées', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque sélection (table Etablissement)')
    afficher_graphique(array1 = timeSelectsUniteLegaleMysql, array2 = timeSelectsUniteLegaleMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'nombre de colonnes sélectionnées', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque sélection (table Unitelegale)')
    afficher_graphique(array1 = timeSelectsEntrepriseMysql, array2 = timeSelectsEntrepriseMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'nombre de colonnes sélectionnées', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque sélection (table Entreprise)')

def plot_graphiques_update():
    timeUpdatesMysql = []
    timeUpdatesEtablissementMysql = []
    timeUpdatesUniteLegaleMysql = []
    timeUpdatesEntrepriseMysql = []
    timeUpdatesMonetdb = []
    timeUpdatesEtablissementMonetdb = []
    timeUpdatesUniteLegaleMonetdb = []
    timeUpdatesEntrepriseMonetdb = []
    for i in range(min(testsMysql.limit, testsMonetdb.limit)):
        requete = "UPDATE TABLENONFRAGMENTEE SET enseigne1etablissement = 'valeurModifiee' WHERE id = " + str(i)
        requete_etablissement = "UPDATE ETABLISSEMENT SET enseigne1etablissement = 'valeurModifiee' WHERE id = " + str(i) 
        requete_unitelegale = "UPDATE UNITELEGALE SET prenom1unitelegale = 'valeurModifiee' WHERE id = " + str(i) 
        requete_entreprise = "UPDATE ENTREPRISE SET categorieentreprise = 'valeurModifiee' WHERE id = " + str(i) 
        save_performance(array = timeUpdatesMysql, cursor = testsMysql.cursor, requete = requete)
        save_performance(array = timeUpdatesEtablissementMysql, cursor = testsMysql.cursor, requete = requete_etablissement)
        save_performance(array = timeUpdatesUniteLegaleMysql, cursor = testsMysql.cursor, requete = requete_unitelegale)
        save_performance(array = timeUpdatesEntrepriseMysql, cursor = testsMysql.cursor, requete = requete_entreprise)
        save_performance(array = timeUpdatesMonetdb, cursor = testsMonetdb.cursor, requete = requete)
        save_performance(array = timeUpdatesEtablissementMonetdb, cursor = testsMonetdb.cursor, requete = requete_etablissement)
        save_performance(array = timeUpdatesUniteLegaleMonetdb, cursor = testsMonetdb.cursor, requete = requete_unitelegale)
        save_performance(array = timeUpdatesEntrepriseMonetdb, cursor = testsMonetdb.cursor, requete = requete_entreprise)

    afficher_graphique(array1 = timeUpdatesMysql, array2 = timeUpdatesMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'id de la ligne mise à jour', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque update (table non fragmentée)')
    afficher_graphique(array1 = timeUpdatesEtablissementMysql, array2 = timeUpdatesEtablissementMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'id de la ligne mise à jour', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque update (table Etablissement)')
    afficher_graphique(array1 = timeUpdatesUniteLegaleMysql, array2 = timeUpdatesUniteLegaleMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'id de la ligne mise à jour', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque update (table Unitelegale)')
    afficher_graphique(array1 = timeUpdatesEntrepriseMysql, array2 = timeUpdatesEntrepriseMonetdb, array1label = 'MySQL', array2label = 'MonetDB', xlabel = 'id de la ligne mise à jour', ylabel = 'temps en secondes', title = 'Temps en secondes de chaque update (table Entreprise)')

def plot_graphiques_update_commun():
    timeUpdatesMysql = []
    timeUpdatesMonetdb = []
    
    requete = "UPDATE TABLENONFRAGMENTEE SET enseigne1etablissement = 'valeurModifiee2'"
    requete_etablissement = "UPDATE ETABLISSEMENT SET enseigne1etablissement = 'valeurModifiee2'"
    requete_unitelegale = "UPDATE UNITELEGALE SET prenom1unitelegale = 'valeurModifiee2'"
    requete_entreprise = "UPDATE ENTREPRISE SET categorieentreprise = 'valeurModifiee2'"
    save_performance(array = timeUpdatesMysql, cursor = testsMysql.cursor, requete = requete)
    save_performance(array = timeUpdatesMysql, cursor = testsMysql.cursor, requete = requete_etablissement)
    save_performance(array = timeUpdatesMysql, cursor = testsMysql.cursor, requete = requete_unitelegale)
    save_performance(array = timeUpdatesMysql, cursor = testsMysql.cursor, requete = requete_entreprise)
    save_performance(array = timeUpdatesMonetdb, cursor = testsMonetdb.cursor, requete = requete)
    save_performance(array = timeUpdatesMonetdb, cursor = testsMonetdb.cursor, requete = requete_etablissement)
    save_performance(array = timeUpdatesMonetdb, cursor = testsMonetdb.cursor, requete = requete_unitelegale)
    save_performance(array = timeUpdatesMonetdb, cursor = testsMonetdb.cursor, requete = requete_entreprise)

    print("Temps en secondes d'un update sans 'WHERE id = ...', donc qui modifie toutes les valeurs :\n")
    print("Mysql :")
    print("Table non fragmentée : ", timeUpdatesMysql[0], 's')
    print("Table Etablissement : ", timeUpdatesMysql[1], 's')
    print("Table Unitelegale : ", timeUpdatesMysql[2], 's')
    print("Table Entreprise : ", timeUpdatesMysql[3], 's\n')
    print("Monetdb :")
    print("Table non fragmentée : ", timeUpdatesMonetdb[0], 's')
    print("Table Etablissement : ", timeUpdatesMonetdb[1], 's')
    print("Table Unitelegale : ", timeUpdatesMonetdb[2], 's')
    print("Table Entreprise : ", timeUpdatesMonetdb[3], 's')

plot_graphiques_insert()

plot_graphiques_select()

plot_graphiques_update()

plot_graphiques_update_commun()