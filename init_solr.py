import requests
import random
import json
from datetime import datetime, timedelta
from faker import Faker
from tqdm import tqdm

# --- CONFIGURATION ---
SOLR_URL = "http://localhost:8983/solr/benchmark_core"
TOTAL_DOCS = 3000
BATCH_SIZE = 500  

fake = Faker('fr_FR')

# Listes de référence pour la génération aléatoire cohérente
DOC_TYPES = ['facture', 'devis', 'incident', 'contrat', 'rapport', 'commande', 'email', 'log', 'projet', 'fiche_paie', 'ticket', 'note_frais', 'dossier_client', 'produit', 'tache']
FOURNISSEURS = ['Haribo', 'Lutti', 'Interne', 'Total', 'Orange', 'Microsoft', 'Google', 'SNCF', 'Apple', 'AWS', 'EDF', 'Bureau Vallée']
STATUTS = ['Validé', 'Clôturé', 'En cours', 'Impayée', 'Brouillon', 'Nouveau', 'En_attente', 'Signé', 'Annulée', 'Rejeté', 'Ouvert', 'Payée']
PRIORITES = ['Basse', 'Moyenne', 'Haute', 'Critique', 'Faible']
PROJETS = ['Alpha', 'Beta', 'Omega', 'Phoenix', 'Migration', 'X', 'Y', 'Z']
LIGNES = ["1", "2", "5", "12", "14"] + [str(i) for i in range(20, 30)]
EMPLOYES = ["Martin", "Sophie", "Durand", "Pierre", "Paul", "Thomas", "Julie"]

# --- 1. FONCTIONS UTILITAIRES ---

def reset_index():
    """Efface toutes les données existantes"""
    print("🧹 Nettoyage de l'index Solr...")
    requests.post(f"{SOLR_URL}/update?commit=true", json={"delete": {"query": "*:*"}})

def setup_schema():
    print("🔧 Configuration du schéma Solr (Strict)...")
    # On définit les champs dynamiques ou explicites
    fields = [
        {"name": "doc_type", "type": "string", "stored": True},
        {"name": "fournisseur", "type": "string", "stored": True},
        {"name": "montant", "type": "pfloat", "stored": True},
        {"name": "date_creation", "type": "pdate", "stored": True},
        {"name": "date_modif", "type": "pdate", "stored": True},
        {"name": "statut", "type": "string", "stored": True},
        {"name": "assigne_a", "type": "string", "stored": True},
        {"name": "projet", "type": "string", "stored": True},
        {"name": "ligne", "type": "string", "stored": True},
        {"name": "priorite", "type": "string", "stored": True},
        {"name": "texte_complet", "type": "text_general", "stored": True},
    ]

    for field in fields:
        payload = {"add-field": field}
        # On ignore les erreurs si le champ existe déjà (400)
        try:
            r = requests.post(f"{SOLR_URL}/schema", json=payload)
        except:
            pass
    print("Schéma configuré.")

def random_date_iso(year=2023, month=None, day=None):
    """Génère une date ISO 8601"""
    if month and day:
        start = datetime(year, month, day)
        end = start + timedelta(days=1)
    elif month:
        start = datetime(year, month, 1)
        # Gestion mois suivant pour range
        if month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, month + 1, 1)
    else:
        start = datetime(year, 1, 1)
        end = datetime.now()
    
    delta = end - start
    random_seconds = random.randrange(int(delta.total_seconds()))
    return (start + timedelta(seconds=random_seconds)).isoformat() + "Z"

def get_relative_date(days_offset):
    """Date relative par rapport à maintenant"""
    return (datetime.now() + timedelta(days=days_offset)).isoformat() + "Z"

# --- 2. GÉNÉRATION CIBLÉE (GOLDEN RECORDS) ---
def generate_golden_records():
    records = []
    
    # --- Q1-Q25 (Base existante) ---
    records.append({"doc_type": "facture", "fournisseur": "Haribo", "montant": 1200, "statut": "Payée", "date_creation": random_date_iso()})
    records.append({"doc_type": "incident", "assigne_a": "Martin", "priorite": "Moyenne", "date_creation": random_date_iso()})
    records.append({"doc_type": "devis", "statut": "Validé", "montant": 450, "date_creation": random_date_iso()})
    records.append({"doc_type": "rapport", "projet": "Alpha", "texte_complet": "Rapport annuel", "priorite": "Haute"})
    records.append({"doc_type": "facture", "montant": 1600, "date_creation": get_relative_date(-20)}) # Mois dernier
    records.append({"doc_type": "incident", "ligne": "12", "sujet": "Incident ligne 12", "statut": "En cours"})
    
    # --- Q26-Q50 (Nouveaux cas) ---
    records.append({"doc_type": "rapport", "projet": "Omega", "texte_complet": "Finalisation Omega"})
    records.append({"doc_type": "commande", "assigne_a": "Martin", "date_creation": random_date_iso()})
    records.append({"doc_type": "facture", "fournisseur": "Orange", "montant": 85.0})
    records.append({"doc_type": "devis", "montant": 300.0}) # < 500
    records.append({"doc_type": "contrat", "montant": 2500.0}) # Entre 2000 et 3000
    records.append({"doc_type": "incident", "date_creation": "2024-01-03T12:00:00Z"}) # Entre 1 et 5 Jan 2024
    records.append({"doc_type": "contrat", "statut": "Brouillon", "date_modif": get_relative_date(-2000)}) # > 5 ans
    records.append({"doc_type": "incident", "statut": "Ouvert", "priorite": "Basse"})
    records.append({"doc_type": "commande", "fournisseur": "Haribo", "date_creation": "2023-06-15T10:00:00Z", "statut": "Validé"}) # 2023 Haribo Pas Annulée
    records.append({"doc_type": "incident", "texte_complet": "Gros problème de panne réseau sur le switch"}) # "panne réseau"
    records.append({"doc_type": "email", "texte_complet": "Merci de respecter la confidentialité des données"}) # "confidentialité"
    records.append({"doc_type": "tache", "priorite": "Critique", "texte_complet": "Urgent task"}) # Ambiguïté Urgent
    records.append({"doc_type": "incident", "ligne": "12", "assigne_a": "Paul"}) # Ligne 12
    records.append({"doc_type": "facture", "fournisseur": "Google", "montant": 150})
    records.append({"doc_type": "projet", "montant": 1500000}) # > 1 Million
    records.append({"doc_type": "rapport", "date_creation": get_relative_date(2)}) # Cette semaine
    records.append({"doc_type": "incident", "priorite": "Critique", "assigne_a": "Pierre"})
    records.append({"doc_type": "log", "texte_complet": "Erreur de connexion base de données"}) # Flou "connexion"

    # --- Q51-Q75 ---
    records.append({"doc_type": "devis", "projet": "Beta", "date_creation": random_date_iso()})
    records.append({"doc_type": "note", "priorite": "Faible"})
    records.append({"doc_type": "produit", "ligne": "5"})
    records.append({"doc_type": "tache", "assigne_a": "Sophie"})
    records.append({"doc_type": "facture", "statut": "En_attente"})
    records.append({"doc_type": "contrat", "montant": 60000}) # > 50k
    records.append({"doc_type": "ticket", "montant": 15.0}) # Entre 10 et 20
    records.append({"doc_type": "commande", "montant": 400.0}) # < 500
    records.append({"doc_type": "log", "date_creation": get_relative_date(-3)}) # Il y a 3 jours
    records.append({"doc_type": "incident", "date_modif": get_relative_date(-10)}) # Depuis début du mois (approx)
    records.append({"doc_type": "rapport", "date_creation": "2019-05-20T10:00:00Z"}) # Avant 2020
    records.append({"doc_type": "ticket", "date_creation": get_relative_date(-1)}) # Hier
    records.append({"doc_type": "facture", "fournisseur": "Interne"})
    records.append({"doc_type": "projet", "projet": "Alpha"}) # Pour tester "Pas Incident"
    records.append({"doc_type": "tache", "assigne_a": "Sophie", "ligne": "2"})
    records.append({"doc_type": "incident", "statut": "Nouveau"}) # Ni Cloturé Ni Rejeté
    records.append({"doc_type": "log", "texte_complet": "Urgence: arrêt machine immédiat"}) # "arrêt machine"
    records.append({"doc_type": "rapport", "texte_complet": "Audit qualité", "priorite": "Haute"})
    records.append({"doc_type": "email", "texte_complet": "Demande de maintnence préventive"}) # Flou typo
    
    # --- Q76-Q100 ---
    records.append({"doc_type": "projet", "montant": 120000}) # > 100k
    records.append({"doc_type": "incident", "ligne": "1", "date_creation": get_relative_date(-5)}) # Récent Ligne 1
    records.append({"doc_type": "facture", "statut": "Brouillon"})
    records.append({"doc_type": "commande", "statut": "Payée"})
    records.append({"doc_type": "facture", "montant": 750}) # Entre 500 et 1000
    records.append({"doc_type": "devis", "date_creation": "2023-04-01T10:00:00Z"}) # Après 15 Mars 2023
    records.append({"doc_type": "email", "texte_complet": "Action immédiat requise"})
    records.append({"doc_type": "rapport", "date_modif": get_relative_date(0)}) # Dernier mis à jour
    records.append({"doc_type": "contrat", "fournisseur": "Haribo", "statut": "En cours"}) # Pas signé
    records.append({"doc_type": "tache", "montant": 45.0}) # Jusqu'à 50 inclus
    records.append({"doc_type": "rapport", "date_creation": "2023-05-15T10:00:00Z"}) # Mois Mai 2023
    records.append({"doc_type": "facture", "texte_complet": "Erreur de facturation"})
    
    # --- Génération de masse pour les TRIS (Sort) ---
    # On ajoute 20 commandes avec des dates et montants variés pour le "Top 10"
    for i in range(20):
        records.append({
            "doc_type": "commande",
            "montant": float(i * 100 + random.randint(1, 99)),
            "date_creation": get_relative_date(-i), # De plus en plus vieux
            "statut": "Validé"
        })
    
    # On ajoute 20 produits pour le tri de prix
    for i in range(20):
        records.append({
            "doc_type": "produit",
            "montant": float(random.randint(5, 5000)),
            "texte_complet": f"Produit {i}"
        })

    return records

# --- 3. GÉNÉRATION DE BRUIT ---
def generate_random_doc():
    return {
        "doc_type": random.choice(DOC_TYPES),
        "fournisseur": random.choice(FOURNISSEURS),
        "montant": round(random.uniform(5.0, 50000.0), 2),
        "date_creation": random_date_iso(year=random.randint(2020, 2024)),
        "date_modif": random_date_iso(year=random.randint(2023, 2024)),
        "statut": random.choice(STATUTS),
        "texte_complet": fake.sentence(nb_words=12),
        "assigne_a": random.choice(EMPLOYES),
        "projet": random.choice(PROJETS),
        "ligne": random.choice(LIGNES),
        "priorite": random.choice(PRIORITES)
    }

# --- 4. EXÉCUTION ---
def main():
    reset_index()
    setup_schema()
    
    all_docs = []
    
    print("Génération des Golden Records...")
    golden = generate_golden_records()
    all_docs.extend(golden)
    
    print(f"Génération du bruit (Total visé: {TOTAL_DOCS})...")
    remaining = TOTAL_DOCS - len(all_docs)
    for _ in range(remaining):
        all_docs.append(generate_random_doc())
    
    print(f"Envoi de {len(all_docs)} documents vers Solr...")
    
    # Envoi par batch
    for i in tqdm(range(0, len(all_docs), BATCH_SIZE)):
        batch = all_docs[i:i + BATCH_SIZE]
        try:
            response = requests.post(
                f"{SOLR_URL}/update?commit=true", 
                json=batch,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code != 200:
                print(f"Erreur Batch {i}: {response.text}")
        except Exception as e:
            print(f"Exception: {e}")

    print("\nIndexation terminée !")
    print(f"Vérifier : http://localhost:8983/solr/#/benchmark_core/query")

if __name__ == "__main__":
    main()