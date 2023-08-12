## <u>Présentation du système NoSQL :</u>

MonetDB est un système de gestion de base de données relationnelle (SGBDR) open-source orienté colonnes. Il est conçu pour fournir des performances élevées lors de requêtes complexes sur de grandes bases de données, telles que la combinaison de tables avec des centaines de colonnes et des millions de lignes. MonetDB a été utilisé dans des applications à haute performance pour le traitement analytique en ligne, l'exploration de données, le système d'information géographique (SIG), le Resource Description Framework (RDF), la recherche de texte et le traitement d'alignement de séquences.

MonetDB dans sa forme actuelle a été créé en 2002 par le doctorant Peter Boncz et le professeur Martin L. Kersten dans le cadre du projet de recherche MAGNUM des années 1990 à l'Université d'Amsterdam.

MonetDB a été développé à l'aide du language de programmation C.

Un SGBD orienté colonne ou SGBD columnaire est un système de gestion de base de données (SGBD) qui stocke les tableaux de données par colonne plutôt que par ligne. Les avantages comprennent un accès plus efficace aux données lorsque l'on interroge seulement un sous-ensemble de colonnes (en éliminant la nécessité de lire les colonnes qui ne sont pas pertinentes), et plus d'options pour la compression des données. Toutefois, ils sont généralement moins efficaces pour l'insertion de nouvelles données.

## <u>Architecture de MonetDB :</u>

L'architecture de MonetDB est représentée en trois couches, chacune avec son propre ensemble d'optimiseurs. Le front-end est la couche supérieure, fournissant une interface de requête pour SQL, avec des interfaces SciQL et SPARQL en cours de développement. 

Les requêtes sont analysées dans des représentations spécifiques au domaine, comme l'algèbre relationnelle pour SQL, et optimisées. Les plans d'exécution logiques générés sont ensuite traduits en instructions en langage d'assemblage MonetDB (MAL), qui sont transmises à la couche suivante. 

La couche intermédiaire ou dorsale fournit un certain nombre d'optimiseurs basés sur les coûts pour le MAL. La couche inférieure est le noyau de la base de données, qui permet d'accéder aux données stockées dans les tables d'association binaires (BAT). Chaque BAT est une table composée d'un identifiant d'objet et de colonnes de valeur, représentant une seule colonne dans la base de données.

La représentation interne des données de MonetDB s'appuie également sur les plages d'adressage de la mémoire des processeurs contemporains en utilisant la pagination à la demande des fichiers mappés en mémoire, s'écartant ainsi des conceptions traditionnelles de SGBD impliquant la gestion complexe de grands magasins de données dans une mémoire limitée.

## <u>Aspects spécifique de MonetDB :</u>

###### <u>Query Recycling :</u>

Le recyclage de requêtes est une architecture permettant de réutiliser les sous-produits du paradigme de l'opérateur à la fois dans un SGBD à stockage de colonnes. Le recyclage utilise l'idée générique de stocker et de réutiliser les résultats de calculs coûteux.

###### <u>Data cracking :</u>

MonetDB a été l'une des premières bases de données à introduire le Database Cracking. Le Database Cracking est une indexation et/ou un tri partiel incrémental des données. Il exploite directement la nature colonnaire de MonetDB. Le "cracking" est une technique qui déplace le coût de la maintenance de l'index des mises à jour vers le traitement des requêtes. Les optimiseurs du pipeline de requêtes sont utilisés pour masser les plans de requêtes à craquer et pour propager cette information. Cette technique permet d'améliorer les temps d'accès et d'obtenir un comportement auto-organisé.

## <u>Comparaison avec les autres SGBD :</u>

Lors de notre recherche, nous avons pu constater que MonetBD n'est pas vraiment le système de gestion de base de données le plus populaire. En effet, lors de notre recherche, qui a été effectué le 1 novembre sur un site référencé dans le sujet, MonetBD se place 131ième au classement des SGBD les plus populaires avec un score de 2,19.

## <u>Références :</u>

- https://hostingdata.co.uk/nosql-database/
- https://db-engines.com/en/ranking
- https://db-engines.com/en/system/MonetDB
- https://en.wikipedia.org/wiki/MonetDB
- https://www.monetdb.org/

