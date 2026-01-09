#!/bin/bash
# check-grafana-setup.sh

echo "Vérification de la configuration Grafana..."
echo ""

# Vérifier la structure des dossiers
echo "1. Structure des dossiers:"
if [ -d "grafana" ]; then
  echo "   ✓ Dossier grafana existe"
  
  if [ -f "grafana/provisioning/datasources/mysql.yml" ]; then
    echo "   ✓ Fichier datasource existe"
  else
    echo "   ✗ Fichier datasource manquant"
  fi
  
  if [ -f "grafana/provisioning/dashboards/dashboard.yml" ]; then
    echo "   ✓ Fichier dashboard config existe"
  else
    echo "   ✗ Fichier dashboard config manquant"
  fi
  
  if [ -f "grafana/dashboards/traffic-urban-dashboard.json" ]; then
    echo "   ✓ Dashboard JSON existe"
  else
    echo "   ✗ Dashboard JSON manquant"
  fi
else
  echo "   ✗ Dossier grafana manquant"
fi

echo ""
echo "2. Vérification Docker Compose:"
if grep -q "./grafana/provisioning:/etc/grafana/provisioning" docker-compose.yml; then
  echo "   ✓ Volume provisioning monté"
else
  echo "   ✗ Volume provisioning non monté"
fi

if grep -q "./grafana/dashboards:/var/lib/grafana/dashboards" docker-compose.yml; then
  echo "   ✓ Volume dashboards monté"
else
  echo "   ✗ Volume dashboards non monté"
fi

echo ""
echo "3. Redémarrer Grafana pour appliquer les changements:"
echo "   docker-compose -f docker-compose-stack-pro.yml restart grafana"
echo ""
echo "4. Accéder à Grafana: http://localhost:3000"
echo "   Login: admin / admin"
echo ""
echo "5. Vérifier dans l'interface Grafana:"
echo "   - Configuration -> Data Sources -> MySQL Traffic doit être présent"
echo "   - Dashboards -> Manage -> 'Dashboard Trafic Urbain - Étape 6' doit être présent"