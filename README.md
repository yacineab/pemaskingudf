# About
Le but de ce projet est d'etendre l'implémentation  de l'UDF Masking Hash de hive aux types de données numérique.
Les types supportés par cette version sont:
**STRING, VARCHAR, CHAR, INT, BIGINT**

# Versions

Artifact | Version
------- | -------
**CDP Version** | 7.1.5
**Java** | 1.8
**Hadoop** | 3.1.1.7.1.6.0-297
**Hive** | 3.1.3000.7.1.6.0-297

# Fonctionnalitées 
- Hash des données de types **STRING, VARCHAR, CHAR** en utilisant l'algorithme *sha256Hex*
- Hash des données de types **Int, BigInt** en implémentant l'algorithme the standard multiplicative hashing algorithm


# Build du projet
`mvn clean install`

### Tests unitaires
`mvn test`

# Utilisation de l'UDF
## Build du projet et chargement le Jar dans HDFS
###### Direct Reference 
1. Upload le jar dans hdfs dans le chemin où les udfs sont stockées.

Exemple:
```shell
hdfs dfs -put pemaskingudf-CDP7.1.5-version-jar-with-dependencies.jar /hdfs/udfpath/
```
## Enregistrer l'UDF

1. En utilisant beeline se connecter à HiveServer avec un user qui a les accès **UDF**
1. Selection la base de données à utiliser
   ```shell
   use default;
   ```

1. Exécutez la commande d'enregistrement qui correspond à la façon dont vous avez configuré le cluster pour trouver le JAR.
Dans le cas de la méthode de configuration JAR **Direct Reference**, vous incluez l'emplacement JAR dans la commande. 
    ```shell
    CREATE FUNCTION pe_mask_udf AS 'PEMaskHashUDF' USING JAR 'hdfs:///udf/hdfspath/pemaskingudf-CDP7.1.5-version-jar-with-dependencies.jar';
    ``` 

    > Si vous utilisez une autre méthode, vous n'incluez pas l'emplacement JAR. Le chargeur de classe peut trouver le fichier JAR.

1. Vérifier que l'udf est enregistrée
    ```shell
    SHOW FUNCTIONS like "%pe%";
   ```   

## Utilisation de l'UDF dans une requête

Supposons que nous avons la table suivante:
```
+---------------+------------------+---------------+---------------------+
| personne.nom  | personne.prenom  | personne.age  |    personne.tel     |
+---------------+------------------+---------------+---------------------+
| ab            | ya               | 10            | 111                 |
| aze           | qsd              | 15            | 333                 |
| dd            | aa               | 111           | 10                  |
| dds           | ava              | 333           | 15                  |
| pre           | nm               | 99            | 999988877755443321  |
+---------------+------------------+---------------+---------------------+
```

### Requete en utilisant l'udf de hashage 
**Requete**
```shell
select default.pe_mask_udf(personne.nom), prenom, default.pe_mask_udf(personne.age),default.pe_mask_udf(personne.tel) from peudf.personne;
```

**Resutat**
```
+-------------------------+---------+--------------+-----------------------+
|          _c0            | prenom  |     _c2      |          _c3          |
+-------------------------+---------+--------------+-----------------------+
| fb8e20fc2e4c3f248c60... | ya      | -383449968   | -1186877602           |
| 9adfb0a6d03beb7141d8... | qsd     | -870655931   | -1409135997           |
| 9b7ecc6eeb83abf9ade1... | aa      | -1186877602  | -383449968            |
| f79fe6c5dd8b31ff18ae... | ava     | -1409135997  | -870655931            |
| 695252f664b93b2375fc... | nm      | -1672646182  | -9185032973038813827  |
+-------------------------+---------+--------------+-----------------------+
```

## Utilisation de l'udf en utilisant Ranger Tag based masking policies
1. Créer la policy sur Ranger
1. Definir le Tag
1. Choisie le role et/ou group et/ou user 
1. Cocher "select" dans *Policy Conditions* 
1. Cocher **HIVE** dans **Component permissions**
1. Choisir **Custom** dans **Select Masking Option** 
1. Entrer le nom de l'udf suivi de **({col})**
    ```
   pe_mask_udf({col})
   ```

> **NB** Le mode opératoire pour l'application de l'udf dans les **Resources based policies** est le même qu'avec les **tag based policies**

# Algorithme Hashing des entiers
> Nous avons utilisé le MurmurHash Algorithm créé par Austin Appleby en 2008
L'algorithme est une implementation de *the standard multiplicative hashing algorithm*

## Algorithme
```
Ha(Key)=(a*K mod W)/(W/M)
```

La valeur **a** est une valeur choisie de manière appropriée qui doit être relativement première à **W**.
Il devrait être grand et sa représentation binaire un mélange aléatoire de 1 et de 0. 
Un cas particulier pratique important se produit lorsque nous choisissant **W** et **M** comme puissance de 2

La formule devient : 
```
Ha(Key)=(aK mod 2^w)/2^(w-n)
```
Ceci est spécial car le modulo arithmétique 2^{w} est effectué par défaut dans les langages de programmation de bas niveau.
La division entière par une puissance de 2 est simplement un décalage vers la droite, donc, en **JAVA**, par exemple, cette fonction devient:
```
Ha(Key) = (a*K) >>> (w-m);
```

>Pour *m* et *w* fixes, cela se traduit par une seule multiplication d'entiers et un décalage vers la droite, ce qui en fait l'une des fonctions de hachage les plus rapides à calculer.

Le hachage multiplicatif est susceptible d'une "erreur commune" qui conduit à une mauvaise diffusion - les bits d'entrée de valeur plus élevée n'affectent pas les bits de sortie de valeur inférieure. 
Pour remédier à cela nous décalons les top-level bits vers la droite (shift-right) et nous effectuons un XORs avec la clé.
On obtient :
```
Ha(Key) = {
Key ^= Key >>> (w-m);
return (a*Key) >>> (w-m);
}
```

### Choix des constantes W et M

> Les constantes ont été choisies dans l'implémentation afin de garantir les trois points:
> - Eviter les collisions
> - Assurer une distribution maximale des clés hashées
> - Garantir l'idempotence

### Int Implementation

```Java
key ^= key >>> 16; 
key *= 0x85ebca6b;
key ^= key >>> 13; 
key *= 0xc2b2ae35;
key ^= key >>> 16; 
return key;
```

### Long Implementation

```Java
key += 0x9e3779b97f4a7c15L;
key ^= (key >>> 30); 
key *= 0xbf58476d1ce4e5b9L;
key ^= (key >>> 27); 
key *= 0x94d049bb133111ebL;
key ^= (key >>> 31);
res = key;
```