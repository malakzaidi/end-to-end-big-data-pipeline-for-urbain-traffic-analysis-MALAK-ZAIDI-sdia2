-- Corriger l'encodage de toutes les tables et données
USE traffic_db;

-- 1. Corriger la base de données
ALTER DATABASE traffic_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 2. Corriger chaque table
ALTER TABLE trafic_par_zone CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE zones_critiques CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE taux_congestion CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE vitesse_moyenne CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE evolution_trafic CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 3. Corriger les données dans zones_critiques
UPDATE zones_critiques 
SET niveau_congestion = CASE
    WHEN niveau_congestion LIKE '%lev%' OR HEX(niveau_congestion) LIKE '%C389%' THEN 'Élevé'
    WHEN niveau_congestion LIKE '%Mod%' OR HEX(niveau_congestion) LIKE '%4D6F64%' THEN 'Modéré'
    ELSE 'Faible'
END;

-- 4. Corriger les données dans trafic_par_zone
UPDATE trafic_par_zone 
SET niveau_trafic = CASE
    WHEN niveau_trafic LIKE '%lev%' OR HEX(niveau_trafic) LIKE '%C389%' THEN 'Élevé'
    ELSE niveau_trafic
END;

-- 5. Corriger les noms de zones (caractères spéciaux)
UPDATE trafic_par_zone 
SET zone = REPLACE(REPLACE(REPLACE(zone, '�', 'é'), '�', 'É'), '�', 'è')
WHERE zone LIKE '%�%';

UPDATE zones_critiques 
SET zone = REPLACE(REPLACE(REPLACE(zone, '�', 'é'), '�', 'É'), '�', 'è')
WHERE zone LIKE '%�%';

-- 6. Vérification
SELECT '=== APRÈS CORRECTION ===' as '';
SELECT 'Zones critiques:' as '';
SELECT zone, niveau_congestion FROM zones_critiques ORDER BY zone;

SELECT 'Trafic par zone:' as '';
SELECT zone, niveau_trafic, trafic_moyen_vehicules_heure FROM trafic_par_zone ORDER BY trafic_moyen_vehicules_heure DESC;
