--- Cleaning Trusted
drop table if exists healthcare.raw.appointments; 
drop table if exists healthcare.raw.patients ;
drop table if exists healthcare.raw.providers ;
drop table if exists healthcare.raw.prescriptions;

--- Cleaning Trusted
drop table if exists healthcare.trusted.appointments; 
drop table if exists healthcare.trusted.patients ;
drop table if exists healthcare.trusted.providers ;
drop table if exists healthcare.trusted.prescriptions;

--- Cleaning Refined
drop table if exists healthcare.refined.appointments; 
drop table if exists healthcare.refined.patients ;
drop table if exists healthcare.refined.providers ;
drop table if exists healthcare.refined.prescriptions;