select 
	p.patient_id,
	p.name,
	p.age,
	p.gender,
	p.registration_date,
	p.age_group,
	p.months_since_registration,
	p.patient_type,
	a.day_of_week,
	a.days_since_last_appointment,
	a.appointment_type
from refined.patients p
left join refined.appointments a on a.patient_id = p.patient_id



select 
	a.appointment_type,
	a.day_of_week,
	p.age_group,
  count(*) as qtt_patients_by_type
from refined.patients p
left join refined.appointments a on a.patient_id = p.patient_id
where appointment_type = 'Emergency'
group by 1, 2 ,3
order by qtt_patients_by_type desc


select 
	p.patient_id,
	p.prescription_id,
	p.prescription_frequency,
	p.medication_category,
	p2.age_group
from refined.prescriptions p 
left join refined.patients p2 on p2.patient_id = p.patient_id


select 
	p.medication_category,
	p2.age_group,
	count(*) as qtd_prescription_by_age
from refined.prescriptions p 
left join refined.patients p2 on p2.patient_id = p.patient_id
group by 1, 2
order by qtd_prescription_by_age desc
--------------

with prescription_freq as (
select 
	p.patient_id,
	p.prescription_frequency
from refined.prescriptions p 
left join refined.patients p2 on p2.patient_id = p.patient_id
),
appointment_freq as (
	select 
		patient_id,
		count(*) as appointment_frequency
	from refined.prescriptions p 
	group by 1
		)
select 
	pf1.patient_id,
	prescription_frequency,
	appointment_frequency ,
	prescription_frequency = appointment_frequency as correlation
from prescription_freq pf1
left join appointment_freq pf2 on pf2.patient_id = pf1.patient_id  

----------------

with finding_the_age as (
select 
	p.patient_id,
	p.provider_id,
	p2.specialty 
from refined.appointments p 
left join refined.providers p2 
	on p.provider_id = p2.provider_id
)
select * from refined.patients p 
left join finding_the_age fta
	on fta.patient_id = p.patient_id