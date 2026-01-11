-- Ce script s'exécute à chaque démarrage de MySQL
USE traffic_db;

-- Nettoyer les noms de zones si jamais ils sont corrompus
UPDATE trafic_par_zone 
SET zone = 
  CASE 
    WHEN zone LIKE '%P%riph%rie_Nord%' THEN 'Périphérie_Nord'
    WHEN zone LIKE '%P%riph%rie_Sud%' THEN 'Périphérie_Sud'
    WHEN zone LIKE '%Quartier_R%sidentiel%' THEN 'Quartier_Résidentiel'
    WHEN zone LIKE '%�%' THEN 
      REPLACE(REPLACE(REPLACE(zone, '�', 'é'), '�', 'è'), '�', 'ê')
    ELSE zone
  END
WHERE zone LIKE '%�%' OR zone LIKE '%P%riph%' OR zone LIKE '%Quartier_R%';

-- Faire la même chose pour les autres tables
UPDATE zones_critiques 
SET zone = 
  CASE 
    WHEN zone LIKE '%P%riph%rie_Nord%' THEN 'Périphérie_Nord'
    WHEN zone LIKE '%P%riph%rie_Sud%' THEN 'Périphérie_Sud'
    WHEN zone LIKE '%Quartier_R%sidentiel%' THEN 'Quartier_Résidentiel'
    WHEN zone LIKE '%�%' THEN 
      REPLACE(REPLACE(REPLACE(zone, '�', 'é'), '�', 'è'), '�', 'ê')
    ELSE zone
  END
WHERE zone LIKE '%�%' OR zone LIKE '%P%riph%' OR zone LIKE '%Quartier_R%';

-- Corriger les niveaux de congestion
UPDATE zones_critiques 
SET niveau_congestion = 
  CASE 
    WHEN niveau_congestion LIKE '%lev%' THEN 'Élevé'
    WHEN niveau_congestion LIKE '%Mod%' THEN 'Modéré'
    ELSE 'Faible'
  END
WHERE niveau_congestion LIKE '%�%' OR niveau_congestion LIKE '%lev%' OR niveau_congestion LIKE '%Mod%';
