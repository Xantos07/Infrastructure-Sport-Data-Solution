-- Script pour réinitialiser complètement la base de données

-- Supprimer toutes les activités
TRUNCATE TABLE activities RESTART IDENTITY CASCADE;

-- Réinitialiser la séquence des IDs
ALTER SEQUENCE activities_id_seq RESTART WITH 1;

-- Vérification
SELECT COUNT(*) as total_activities FROM activities;
