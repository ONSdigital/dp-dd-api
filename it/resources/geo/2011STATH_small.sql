--- imported using dp-dd-hierarchy-importer from http://web.ons.gov.uk/ons/api/data/hierarchies/hierarchy/2011STATH.json?apikey=XXXXXXX&levels=0,1,2,3,4,5,6,7,8,9insert into hierarchy (hierarchy_id, hierarchy_name, hierarchy_type) values ('2011STATH', '2011 Statistical Geography Hierarchy', 'geography');
insert into hierarchy (id, name, type) values ('2011STATH', '2011 STATH', 'geography');

insert into hierarchy_level_type (id, name, level) values ('CA', 'Council Area', 4) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('CTRY', 'Country', 3) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('GB', 'Great Britain', 1) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('LONB', 'London Borough ', 5) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('LSOA', 'Lower Layer Super Output Area', 7) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('MD', 'Metropolitan District ', 5) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('MSOA', 'Middle Layer Super Output Area', 6) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('NAT', 'England and Wales', 2) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('NMD', 'Non-metropolitan District', 5) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('OA', 'Output Area', 8) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('RGN', 'Region', 4) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('UA', 'Unitary Authority', 5) on conflict do nothing;
insert into hierarchy_level_type (id, name, level) values ('UK', 'United Kingdom', 0) on conflict do nothing;

insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'K02000001', null, 'United Kingdom', 'UK', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'K03000001', 'K02000001', 'Great Britain', 'GB', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'K04000001', 'K03000001', 'England and Wales', 'NAT', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E92000001', 'K04000001', 'England', 'CTRY', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E12000007', 'E92000001', 'London', 'RGN', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E09000001', 'E12000007', 'City of London', 'LONB', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E02000001', 'E09000001', 'City of London 001', 'MSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E01000001', 'E02000001', 'City of London 001A', 'LSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000001', 'E01000001', 'E00000001', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000003', 'E01000001', 'E00000003', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000005', 'E01000001', 'E00000005', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000007', 'E01000001', 'E00000007', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E01000003', 'E02000001', 'City of London 001C', 'LSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000010', 'E01000003', 'E00000010', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000012', 'E01000003', 'E00000012', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000013', 'E01000003', 'E00000013', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000014', 'E01000003', 'E00000014', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E01000002', 'E02000001', 'City of London 001B', 'LSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000016', 'E01000002', 'E00000016', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000017', 'E01000002', 'E00000017', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000018', 'E01000002', 'E00000018', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000019', 'E01000002', 'E00000019', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000020', 'E01000002', 'E00000020', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000021', 'E01000002', 'E00000021', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000022', 'E01000003', 'E00000022', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000023', 'E01000003', 'E00000023', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E01032739', 'E02000001', 'City of London 001F', 'LSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000024', 'E01032739', 'E00000024', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E01032740', 'E02000001', 'City of London 001G', 'LSOA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000025', 'E01032740', 'E00000025', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000026', 'E01032740', 'E00000026', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000027', 'E01032740', 'E00000027', 'OA', 0);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('2011STATH', 'E00000028', 'E01032740', 'E00000028', 'OA', 0);
