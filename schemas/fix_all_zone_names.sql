USE traffic_db;

-- Fonction pour corriger TOUS les caractères spéciaux
UPDATE trafic_par_zone 
SET zone = 
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(zone, '�', 'é'),
        '�', 'è'
      ),
      'P�riph�rie', 'Périphérie'
    ),
    'Quartier_R�sidentiel', 'Quartier_Résidentiel'
  );

UPDATE zones_critiques 
SET zone = 
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(zone, '�', 'é'),
        '�', 'è'
      ),
      'P�riph�rie', 'Périphérie'
    ),
    'Quartier_R�sidentiel', 'Quartier_Résidentiel'
  );

UPDATE taux_congestion 
SET zone = 
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(zone, '�', 'é'),
        '�', 'è'
      ),
      'P�riph�rie', 'Périphérie'
    ),
    'Quartier_R�sidentiel', 'Quartier_Résidentiel'
  );

UPDATE vitesse_moyenne 
SET zone = 
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(zone, '�', 'é'),
        '�', 'è'
      ),
      'P�riph�rie', 'Périphérie'
    ),
    'Quartier_R�sidentiel', 'Quartier_Résidentiel'
  );

-- Alternative: méthode universelle (corrige tous les cas)
UPDATE trafic_par_zone 
SET zone = 
  CASE
    WHEN zone = 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN zone = 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN zone = 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    WHEN zone LIKE '%�%' THEN REPLACE(REPLACE(zone, '�', 'é'), '�', 'è')
    ELSE zone
  END;

-- Vérification
SELECT '=== ZONES CORRIGÉES ===' as '';
SELECT 'trafic_par_zone:' as '';
SELECT DISTINCT zone FROM trafic_par_zone ORDER BY zone;

SELECT '=== DONNÉES FINALES ===' as '';
SELECT 
  zone,
  niveau_trafic,
  trafic_moyen_vehicules_heure,
  ROW_NUMBER() OVER (ORDER BY trafic_moyen_vehicules_heure DESC) as rang
FROM trafic_par_zone 
ORDER BY trafic_moyen_vehicules_heure DESC;
