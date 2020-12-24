beginning_of_time = '2000-01-01 00:00:00'
end_of_time = '2199-12-31 23:59:59'

# Parameters for the table queries ??
ash_content_tag = 'AN027IA'
hopper_outflow_tag = 'OLScaleTPH'

# LoaderTracker
speed_window_semisize_sec = 15
speed_slow_lim = 1.25

# Hopper
hopper_flow_interpolation_margin_minutes = 5
hopper_level_interpolation_margin_minutes = 5

# Prediction
indep_locvar_characterization_const_dummy_trust_radius = 15

# Recommendation
recomm_current_ash_stats_duration_min = 60
recomm_ash_w = 1
recomm_trust_w = 1

historian_db_name = 'historian'
rcb_db_name = 'rcb'
wenco_db_name = 'wenco'

equipment_table = "equip"
equipment_coords_table = "equip_coord_trans"