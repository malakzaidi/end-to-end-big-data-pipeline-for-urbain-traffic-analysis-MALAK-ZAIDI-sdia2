USE traffic_db;

-- Corriger TOUS les noms de zones dans TOUTES les tables
UPDATE trafic_par_zone 
SET zone = 
  CASE zone
    WHEN 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    ELSE zone
  END;

UPDATE zones_critiques 
SET zone = 
  CASE zone
    WHEN 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    ELSE zone
  END;

UPDATE taux_congestion 
SET zone = 
  CASE zone
    WHEN 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    ELSE zone
  END;

UPDATE vitesse_moyenne 
SET zone = 
  CASE zone
    WHEN 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    ELSE zone
  END;

UPDATE evolution_trafic 
SET zone = 
  CASE zone
    WHEN 'P�riph�rie_Nord' THEN 'Périphérie_Nord'
    WHEN 'P�riph�rie_Sud' THEN 'Périphérie_Sud'
    WHEN 'Quartier_R�sidentiel' THEN 'Quartier_Résidentiel'
    ELSE zone
  END;

-- Vérification finale
SELECT '=== NOMS DE ZONES CORRIGÉS ===' as '';
SELECT DISTINCT zone FROM trafic_par_zone ORDER BY zone;

SELECT '=== MÉTRIQUES FINALES ===' as '';
SELECT 
  'Trafic Total' as metric,
  ROUND(SUM(trafic_moyen_vehicules_heure), 2) as value
FROM trafic_par_zone

UNION ALL

SELECT 
  'Zones Critiques' as metric,
  COUNT(*) as value
FROM zones_critiques 
WHERE niveau_congestion != 'Faible'

UNION ALL

SELECT 
  'Congestion Moyenne' as metric,
  ROUND(AVG(taux_congestion_pourcentage), 2) as value
FROM taux_congestion

UNION ALL

SELECT 
  'Vitesse Moyenne' as metric,
  ROUND(AVG(vitesse_moyenne_kmh), 2) as value
FROM vitesse_moyenne;
