import requests
import random
import json
from datetime import datetime, timedelta
from faker import Faker
from tqdm import tqdm

# --- CONFIGURATION ---
SOLR_URL = "http://localhost:8983/solr/benchmark_core"
TOTAL_DOCS = 1000  # Nombre total de documents à générer
BATCH_SIZE = 250    # Envoi par paquets pour ne pas saturer le réseau

fake = Faker('fr_FR') # Générateur de données françaises

# --- 1. DÉFINITION DU SCHÉMA COMPLET ---
# On s'assure que Solr connait tous les champs utilisés dans tes questions
def setup_schema():
    print("🔧 Configuration du schéma Solr...")
    fields = [
        {"name": "doc_type", "type": "string", "stored": True},
        {"name": "fournisseur", "type": "string", "stored": True},
        {"name": "client", "type": "string", "stored": True},
        {"name": "assigne_a", "type": "string", "stored": True},
        {"name": "statut", "type": "string", "stored": True},
        {"name": "projet", "type": "string", "stored": True},
        {"name": "priorite", "type": "string", "stored": True},
        {"name": "categorie", "type": "string", "stored": True},
        {"name": "sujet", "type": "text_general", "stored": True},
        {"name": "ligne", "type": "string", "stored": True},
        {"name": "montant", "type": "pfloat", "stored": True},
        {"name": "date_creation", "type": "pdate", "stored": True},
        {"name": "date_modif", "type": "pdate", "stored": True},
        {"name": "texte_complet", "type": "text_general", "stored": True}, # Recherche floue
    ]

    for field in fields:
        payload = {"add-field": field}
        # On ignore les erreurs si le champ existe déjà
        requests.post(f"{SOLR_URL}/schema", json=payload)
    print("✅ Schéma configuré.")

# --- 2. FONCTIONS UTILITAIRES ---
def random_date(start_year=2020):
    """Génère une date aléatoire ISO"""
    start = datetime(start_year, 1, 1)
    end = datetime.now()
    delta = end - start
    random_days = random.randrange(delta.days)
    return (start + timedelta(days=random_days)).isoformat() + "Z"

def get_recent_date(days=7):
    """Date récente pour les requêtes 'semaine dernière'"""
    return (datetime.now() - timedelta(days=random.randint(1, days))).isoformat() + "Z"

# --- 3. GÉNÉRATION DES DONNÉES CIBLÉES (Golden Records) ---
# Ce sont les réponses exactes à tes 25 questions
def generate_golden_records():
    records = [
        # Q1 & Q13: Haribo et Lutti
        {"doc_type": "facture", "fournisseur": "Haribo", "montant": 1200, "statut": "Payée", "date_creation": random_date()},
        {"doc_type": "facture", "fournisseur": "Lutti", "montant": 800, "statut": "En cours", "date_creation": random_date()},
        
        # Q2 & Q23: Contrats et Gros contrats
        {"doc_type": "Contrat", "montant": 45000, "texte_complet": "Contrat standard", "date_creation": random_date()},
        {"doc_type": "Contrat", "montant": 60000, "texte_complet": "Contrat cadre global", "date_creation": (datetime.now() - timedelta(days=360)).isoformat() + "Z"}, # Année dernière
        
        # Q3: Incident Martin
        {"doc_type": "incident", "assigne_a": "Martin", "priorite": "Moyenne", "sujet": "Panne réseau", "date_creation": random_date()},
        
        # Q4: Devis Validé & Q21 Tri
        {"doc_type": "devis", "statut": "Validé", "date_creation": "2023-01-01T10:00:00Z"},
        {"doc_type": "devis", "statut": "Validé", "date_creation": "2023-06-01T10:00:00Z"},
        
        # Q5: Projet Alpha
        {"doc_type": "rapport", "projet": "Alpha", "texte_complet": "Avancement Q1", "date_creation": random_date()},
        
        # Q6 & Q18: Factures chères
        {"doc_type": "facture", "montant": 1600, "fournisseur": "EDF", "date_creation": random_date()},
        {"doc_type": "facture", "montant": 5000, "fournisseur": "AWS", "date_creation": random_date()},
        
        # Q7: Mois dernier
        {"doc_type": "facture", "montant": 300, "date_creation": (datetime.now() - timedelta(days=20)).isoformat() + "Z"},
        
        # Q8: Commandes range
        {"doc_type": "commande", "montant": 350, "date_creation": random_date()},
        
        # Q9: Avant 2024
        {"doc_type": "incident", "sujet": "Vieux bug", "date_creation": "2023-12-31T23:59:59Z"},
        
        # Q10 & Q25: Modifiés récemment / Maintenance
        {"doc_type": "rapport", "date_modif": get_recent_date(3), "texte_complet": "Maintenance serveur hebdomadaire"},
        
        # Q11: Facture < 100
        {"doc_type": "facture", "montant": 50.50, "fournisseur": "Bureau Vallée", "date_creation": random_date()},
        
        # Q12: Incident Ligne 12 non clôturé
        {"doc_type": "incident", "sujet": "Incident", "ligne": "12", "statut": "En cours", "date_creation": random_date()},
        {"doc_type": "incident", "sujet": "Incident", "ligne": "12", "statut": "Clôturé", "date_creation": random_date()}, # Le piège
        
        # Q14: Urgent pas facture
        {"doc_type": "email", "texte_complet": "C'est très Urgent merci", "date_creation": random_date()},
        
        # Q15: Priorité Haute Ligne 14
        {"doc_type": "incident", "ligne": "14", "priorite": "Critique", "date_creation": random_date()},
        
        # Q16: Tout sauf Interne
        {"doc_type": "note", "fournisseur": "Interne", "texte_complet": "Note de service", "date_creation": random_date()},
        
        # Q17: Fuite rapport maintenance
        {"doc_type": "rapport", "categorie": "maintenance", "texte_complet": "Détection d'une fuite d'eau", "date_creation": random_date()},
        
        # Q20: Phrase exacte
        {"doc_type": "log", "texte_complet": "Attention erreur système critique détectée au démarrage", "date_creation": random_date()},
        
        # Q22: Sécurité récents
        {"doc_type": "audit", "texte_complet": "Analyse des problèmes de sécurité", "date_creation": get_recent_date(15)},
        
        # Q24: Impayée Client X
        {"doc_type": "facture", "statut": "Impayée", "client": "X", "montant": 2000, "date_creation": random_date()},
    ]
    return records

# --- 4. GÉNÉRATION DE BRUIT (Random Data) ---
def generate_random_batch(size):
    batch = []
    types = ['facture', 'devis', 'incident', 'contrat', 'rapport', 'commande', 'email', 'log']
    fournisseurs = ['Haribo', 'Lutti', 'Interne', 'Total', 'Orange', 'Microsoft', 'Google', 'SNCF']
    statuts = ['Validé', 'Clôturé', 'En cours', 'Impayée', 'Brouillon', 'Nouveau']
    priorites = ['Basse', 'Moyenne', 'Haute', 'Critique']
    projets = ['Alpha', 'Beta', 'Omega', 'Phoenix', 'Migration']
    
    for _ in range(size):
        doc = {
            "doc_type": random.choice(types),
            "fournisseur": random.choice(fournisseurs),
            "montant": round(random.uniform(10.0, 10000.0), 2),
            "date_creation": random_date(),
            "date_modif": random_date(),
            "statut": random.choice(statuts),
            "texte_complet": fake.sentence(nb_words=10),
            "sujet": fake.sentence(nb_words=5),
            "assigne_a": fake.first_name(),
            "projet": random.choice(projets),
            "ligne": str(random.randint(1, 20)),
            "priorite": random.choice(priorites),
            "client": fake.company(),
            "categorie": fake.word()
        }
        batch.append(doc)
    return batch

# --- 5. ORCHESTRATION ---
def main():
    setup_schema()
    
    all_docs = []
    
    print("💎 Génération des 'Golden Records' (Données ciblées)...")
    golden = generate_golden_records()
    all_docs.extend(golden)
    
    print(f"🎲 Génération de {TOTAL_DOCS} documents aléatoires...")
    noise_needed = TOTAL_DOCS - len(golden)
    noise = generate_random_batch(noise_needed)
    all_docs.extend(noise)
    
    print(f"🚀 Envoi de {len(all_docs)} documents vers Solr...")
    
    # Envoi par batch
    for i in tqdm(range(0, len(all_docs), BATCH_SIZE)):
        batch = all_docs[i:i + BATCH_SIZE]
        try:
            # commit=true seulement à la fin pour la perf, mais ici on le fait à chaque batch pour être sûr
            response = requests.post(
                f"{SOLR_URL}/update?commit=true", 
                json=batch,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code != 200:
                print(f"❌ Erreur Batch {i}: {response.text}")
        except Exception as e:
            print(f"❌ Exception: {e}")

    print("\n✨ Terminé ! Ta base Solr est peuplée.")
    print(f"👉 Vérifie ici : http://localhost:8983/solr/#/benchmark_core/query")

if __name__ == "__main__":
    main()