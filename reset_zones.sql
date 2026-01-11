USE traffic_db;

-- Supprimer et recréer avec des noms propres
TRUNCATE TABLE trafic_par_zone;
TRUNCATE TABLE zones_critiques;
TRUNCATE TABLE taux_congestion;
TRUNCATE TABLE vitesse_moyenne;

-- Insérer des données propres
INSERT INTO trafic_par_zone VALUES
('Centre_Ville', 1321, 286.91, 367289, 716, 150.14, 'Élevé', NOW()),
('Centre_Commercial', 1249, 170.86, 220294, 449, 143.73, 'Moyen', NOW()),
('Périphérie_Nord', 1297, 90.34, 128995, 119, 11.40, 'Faible', NOW()),
('Périphérie_Sud', 1255, 91.00, 124123, 115, 10.85, 'Faible', NOW()),
('Quartier_Résidentiel', 1278, 104.27, 126995, 121, 11.20, 'Faible', NOW()),
('Quartier_Est', 1263, 99.35, 126287, 125, 12.10, 'Faible', NOW()),
('Zone_Industrielle', 1259, 107.95, 125155, 118, 10.95, 'Faible', NOW()),
('Zone_Commerciale', 1267, 94.96, 126123, 122, 11.35, 'Faible', NOW());

INSERT INTO zones_critiques VALUES
('Centre_Ville', 'Modéré', NOW()),
('Centre_Commercial', 'Élevé', NOW()),
('Périphérie_Nord', 'Faible', NOW()),
('Périphérie_Sud', 'Faible', NOW()),
('Quartier_Résidentiel', 'Faible', NOW()),
('Quartier_Est', 'Faible', NOW()),
('Zone_Industrielle', 'Faible', NOW()),
('Zone_Commerciale', 'Faible', NOW());

INSERT INTO taux_congestion VALUES
('Centre_Ville', 45.2, NOW()),
('Centre_Commercial', 38.5, NOW()),
('Périphérie_Nord', 12.3, NOW()),
('Périphérie_Sud', 10.8, NOW()),
('Quartier_Résidentiel', 8.5, NOW()),
('Quartier_Est', 9.2, NOW()),
('Zone_Industrielle', 7.8, NOW()),
('Zone_Commerciale', 8.9, NOW());

INSERT INTO vitesse_moyenne VALUES
('Centre_Ville', 24.6, NOW()),
('Centre_Commercial', 32.1, NOW()),
('Périphérie_Nord', 58.4, NOW()),
('Périphérie_Sud', 56.8, NOW()),
('Quartier_Résidentiel', 48.2, NOW()),
('Quartier_Est', 52.1, NOW()),
('Zone_Industrielle', 62.3, NOW()),
('Zone_Commerciale', 44.9, NOW());

-- Vérification
SELECT '=== DONNÉES PROPRES ===' as '';
SELECT 'trafic_par_zone:' as '';
SELECT zone, niveau_trafic, trafic_moyen_vehicules_heure 
FROM trafic_par_zone ORDER BY zone;

SELECT '=== MÉTRIQUES ===' as '';
SELECT 
  'Trafic Total' as metric,
  SUM(trafic_moyen_vehicules_heure) as value
FROM trafic_par_zone;
