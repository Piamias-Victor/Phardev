# Phardev

Phardev est un projet Django qui permet de gérer l'inventaire, la création des commandes, et des ventes pour différents types de logiciels partenaires. Le projet intègre différentes vues pour chaque opération et offre des outils pour gérer les processus d'insertion dans la base de données.

## Structure du Projet

### 1. **Dossier `data`**
Le dossier `data` contient les vues Django qui gèrent les différentes opérations. Ces vues sont organisées par type de logiciel partenaire et par type d'opération.

Les opérations disponibles sont :
- Vérification de l'inventaire
- Création des commandes
- Création des ventes

Pour chaque type de logiciel partenaire (Dexter et Winpharma), il existe une vue correspondante pour chaque opération. Cela donne un total de **6 vues** au sein du projet :
1. Vue pour vérifier l'inventaire (Dexter)
2. Vue pour vérifier l'inventaire (Winpharma)
3. Vue pour créer une commande (Dexter)
4. Vue pour créer une commande (Winpharma)
5. Vue pour créer une vente (Dexter)
6. Vue pour créer une vente (Winpharma)

### 2. **Dossier `utils`**
Le dossier `utils` contient un fichier nommé `process.py` qui regroupe la logique d'insertion dans la base de données. Ce fichier est utilisé pour effectuer des opérations d'insertion de données dans la base, avec une gestion spécifique à chaque type de logiciel partenaire.

## Fonctionnalités
- **Vérification de l'inventaire :** Permet de vérifier les niveaux de stock pour les logiciels Dexter et Winpharma.
- **Création de commandes :** Permet de créer des commandes pour les logiciels Dexter et Winpharma.
- **Création de ventes :** Permet de créer des ventes pour les logiciels Dexter et Winpharma.
- **Insertion dans la base de données :** Utilise la logique définie dans le fichier `process.py` pour insérer des données dans la base de données en fonction des opérations réalisées.

## Installation

1. Clonez ce repository :
   ```bash
   git clone https://github.com/nom-utilisateur/phardev.git
   ```

2. Installez les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

3. Appliquez les migrations pour configurer la base de données :
   ```bash
   python manage.py migrate
   ```

4. Lancez le serveur de développement :
   ```bash
   python manage.py runserver
   ```

## Contributions

Les contributions sont les bienvenues. N'hésitez pas à soumettre des issues et des pull requests pour améliorer le projet.

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.
