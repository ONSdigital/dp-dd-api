-- imported using dp-dd-hierarchy-importer from http://web.ons.gov.uk/ons/api/data/classification/CL_0000641.json?apikey=XXXXXXXX&context=Economic

insert into hierarchy (id, name, type) values ('CL_0000641', 'Special Aggregate', 'classification');


insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004216', null, 'CPI (overall index)', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004331', null, '01 Food and non-alcoholic beverages', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004217', 'CI_0004331', '01.1 Food', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004218', 'CI_0004331', '01.2 Non-alcoholic beverages', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004332', null, '02 Alcoholic beverages and tobacco', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004219', 'CI_0004332', '02.1 Alcoholic beverages', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004220', 'CI_0004332', '02.2 Tobacco', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004333', null, '03 Clothing and footwear', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004221', 'CI_0004333', '03.1 Clothing', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004222', 'CI_0004333', '03.2 Footwear including repairs', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004334', null, '04 Housing, water, electricity, gas and other fuels', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004223', 'CI_0004334', '04.1 Actual rentals for housing', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004224', 'CI_0004334', '04.3 Regular maintenance and repair of the dwelling', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004225', 'CI_0004334', '04.4 Water supply and misc. services for the dwelling', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004226', 'CI_0004334', '04.5 Electricity , gas and other fuels', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004335', null, '05 Furniture, household equipment and maintenance', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004227', 'CI_0004335', '05.1 Furniture, furnishings and carpets', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004228', 'CI_0004335', '05.2 Household textiles', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004229', 'CI_0004335', '05.3 Household appliances, fitting and repairs', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004230', 'CI_0004335', '05.4 Glassware, tableware and household utensils', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004231', 'CI_0004335', '05.5 Tools and equipment for house and garden', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004232', 'CI_0004335', '05.6 Goods and services for routine maintenance', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004336', null, '06 Health', null, 7);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004233', 'CI_0004336', '06.1 Medical products, appliances and equipment', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004337', null, '07 Transport', null, 8);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004234', 'CI_0004337', '07.1 Purchase of vehicles', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004235', 'CI_0004337', '07.2 Operation of personal transport equipment', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004236', 'CI_0004337', '07.3 Transport services', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004338', null, '08 Communication', null, 9);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004237', 'CI_0004338', '08.1 Postal services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004238', 'CI_0004338', '08.2/3 Telephone and telefax equip', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004339', null, '09 Recreation and culture', null, 10);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004239', 'CI_0004339', '09.1 Audio-visual equipment and related products', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004240', 'CI_0004339', '09.3 Other recreational items, gardens and pets', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004241', 'CI_0004339', '09.4 Recreational and cultural services', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004242', 'CI_0004339', '09.5 Books, newspapers and stationery', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004243', 'CI_0004339', '09.6 Package holidays', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004340', null, '10 Education', null, 11);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004244', 'CI_0004340', '10.0 Education', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004341', null, '11 Restaurants and hotels', null, 12);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004245', 'CI_0004341', '11.1 Catering services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004246', 'CI_0004341', '11.2 Accommodation services', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004342', null, '12 Miscellaneous goods and services', null, 13);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004247', 'CI_0004342', '12.1 Personal care', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004248', 'CI_0004342', '12.3 Personal effects (nec)', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004249', 'CI_0004342', '12.5 Insurance', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004250', 'CI_0004342', '12.6 Financial services (nec)', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004251', 'CI_0004342', '12.7 Other services (nec)', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004252', 'CI_0004217', '01.1.1 Bread and cereals', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004253', 'CI_0004217', '01.1.2 Meat', null, 9);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004254', 'CI_0004217', '01.1.3 Fish', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004255', 'CI_0004217', '01.1.4 Milk, cheese and eggs', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004256', 'CI_0004217', '01.1.5 Oils and fats', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004257', 'CI_0004217', '01.1.6 Fruit', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004258', 'CI_0004217', '01.1.7 Vegetables including potatoes and tubers', null, 7);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004259', 'CI_0004217', '01.1.8 Sugar, jam, syrups, chocolate and confectionery', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004260', 'CI_0004217', '01.1.9 Food products (nec)', null, 8);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004261', 'CI_0004218', '01.2.1 Coffee, tea and cocoa', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004262', 'CI_0004218', '01.2.2 Mineral waters, soft drinks and juices', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004263', 'CI_0004219', '02.1.1 Spirits', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004264', 'CI_0004219', '02.1.2 Wine', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004265', 'CI_0004219', '02.1.3 Beer', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004266', 'CI_0004220', '02.2.0 Tobacco', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004267', 'CI_0004221', '03.1.2 Garments', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004268', 'CI_0004221', '03.1.3 Other clothing and clothing accessories', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004269', 'CI_0004221', '03.1.4 Cleaning, repair and hire of clothing', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004270', 'CI_0004222', '03.2.0 Footwear including repairs', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004271', 'CI_0004223', '04.1.0 Actual rentals for housing', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004272', 'CI_0004224', '04.3.1 Materials for maintenance and repair', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004273', 'CI_0004224', '04.3.2 Services for maintenance and repair', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004274', 'CI_0004225', '04.4.1 Water supply', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004275', 'CI_0004225', '04.4.3 Sewerage collection', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004276', 'CI_0004226', '04.5.1 Electricity', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004277', 'CI_0004226', '04.5.2 Gas', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004278', 'CI_0004226', '04.5.3 Liquid fuels', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004279', 'CI_0004226', '04.5.4 Solid fuels', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004280', 'CI_0004227', '05.1.1 Furniture and furnishings', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004281', 'CI_0004227', '05.1.2 Carpets and other floor coverings', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004282', 'CI_0004228', '05.2.0 Household Textiles', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004283', 'CI_0004229', '05.3.1/2 Major appliances and small electric goods', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004284', 'CI_0004229', '05.3.3 Repair of household appliances', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004285', 'CI_0004230', '05.4.0 Glassware, Tableware and Household Utensils', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004286', 'CI_0004231', '05.5.0 Tools and equipment for House and Garden', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004287', 'CI_0004232', '05.6.1 Non-durable household goods', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004288', 'CI_0004232', '05.6.2 Domestic services and household services', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004289', 'CI_0004233', '06.1.1 Pharmaceutical products', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004290', 'CI_0004233', '06.1.2/3 Other medical and therapeutic equipment', null, 12);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004291', 'CI_0004234', '07.1.2/3 Motorcycles and bicycles', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004292', 'CI_0004234', '07.1.1 New Cars', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004293', 'CI_0004234', '07.1.1b Second Hand Cars', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004294', 'CI_0004235', '07.2.1 Spare parts and accessories', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004295', 'CI_0004235', '07.2.2 Fuels and lubricants', null, 11);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004296', 'CI_0004235', '07.2.3 Maintenance and repairs', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004297', 'CI_0004235', '07.2.4 Other services', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004298', 'CI_0004236', '07.3.1 Passenger transport by railway', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004299', 'CI_0004236', '07.3.2 Passenger transport by road', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004300', 'CI_0004236', '07.3.3 Passenger transport by air', null, 23);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004301', 'CI_0004236', '07.3.4 Passenger transport by sea and inland waterway', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004302', 'CI_0004237', '08.1.0 Postal Services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004303', 'CI_0004238', '08.2.0 Telephone and Telefax Equipment & Services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004304', 'CI_0004239', '09.1.1 Reception and reproduction of sound and pictures', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004305', 'CI_0004239', '09.1.2 Photographic, cinematographic and optical equipment', null, 4);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004306', 'CI_0004239', '09.1.3 Data processing equipment', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004307', 'CI_0004239', '09.1.4 Recording media', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004308', 'CI_0004239', '09.1.5 Repair of audio-visual equipment , related products', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004309', 'CI_0004240', '09.3.1 Games, toys and hobbies', null, 19);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004310', 'CI_0004240', '09.3.2 Equipment for sport and open-air recreation', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004311', 'CI_0004240', '09.3.3 Gardens, plants and flowers', null, 14);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004312', 'CI_0004240', '09.3.4/5 Pets, related products and services', null, 21);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004313', 'CI_0004241', '09.4.1 Recreational and sporting services', null, 15);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004314', 'CI_0004241', '09.4.2 Cultural services', null, 5);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004315', 'CI_0004242', '09.5.1 Books', null, 6);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004316', 'CI_0004242', '09.5.2 Newspapers and periodicals', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004317', 'CI_0004242', '09.5.3/4 Misc. printed matter, stationery, drawing materials', null, 18);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004318', 'CI_0004243', '09.6.0 Package Holidays', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004319', 'CI_0004244', '10.0.0 Education', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004320', 'CI_0004245', '11.1.1 Restaurants, cafes', null, 17);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004321', 'CI_0004245', '11.1.2 Canteens', null, 8);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004322', 'CI_0004246', '11.2.0 Accommodation Services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004323', 'CI_0004247', '12.1.1 Hairdressing and personal grooming establishments', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004324', 'CI_0004247', '12.1.2/3 Appliances and products for personal care', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004325', 'CI_0004248', '12.3.1 Jewellery, clocks and watches', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004326', 'CI_0004248', '12.3.2 Other personal effects', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004327', 'CI_0004249', '12.5.2 House contents insurance', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004328', 'CI_0004249', '12.5.4 Transport insurance', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004329', 'CI_0004250', '12.6.2 Other financial services (nec)', null, 9);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004330', 'CI_0004251', '12.7.0 Other Services Not Elsewhere covered', null, 7);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004354', 'CI_0004336', '06.2 Out-patient services', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004355', 'CI_0004336', '06.3 Hospital services', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004356', 'CI_0004339', '9.2 Other major durables for recreation and culture', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004357', 'CI_0004342', '12.4 Social protection', null, 3);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004358', 'CI_0004354', '06.2.1/3 Medical services, paramedical services', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004359', 'CI_0004354', '06.2.2 Dental services', null, 13);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004360', 'CI_0004355', '06.3.0 Hospital Services', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004361', 'CI_0004356', '09.2.1/2 Major durables for in/outdoor recreation', null, 1);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004362', 'CI_0004357', '12.4.0 Social Protection', null, 10);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0004363', 'CI_0004249', '12.5.3 Health insurance', null, 2);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005156', null, '910000 Goods', null, 14);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005157', null, '911000 Food, Alcoholic Beverages & Tobacco', null, 15);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005158', null, '911100 Processed Food & Non-Alcoholic Beverages', null, 16);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005159', null, '911200 Non-processed Food', null, 17);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005160', null, '911210 Seasonal Food', null, 18);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005161', null, '911300 cpict alcohol & tobacco', null, 19);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005163', null, '912000 Industrial Goods', null, 20);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005164', null, '912100 Energy', null, 21);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005165', null, '912110 Electricity, Gas & Misc. Energy', null, 22);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005166', null, '912120 Liquid Fuels, Vehicle Fuels & Lubricants', null, 23);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005167', null, '912200 Non-Energy Industrial Goods', null, 24);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005168', null, '912210 Clothing & Footwear Goods', null, 25);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005169', null, '912220 Housing Goods', null, 26);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005170', null, '912221 Household goods', null, 27);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005171', null, '912222 Water supply; materials for maintenance/repair', null, 28);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005172', null, '912230 Medical products, appliances and equipment', null, 29);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005173', null, '912240 Vehicles, spare parts and accessories', null, 30);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005174', null, '912250 Recreational Goods', null, 31);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005175', null, '912251 Audio-Visual Goods', null, 32);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005176', null, '912252 Other Recreational Goods', null, 33);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005177', null, '912260 Miscellaneous Goods', null, 34);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005178', null, '920000 Services', null, 35);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005179', null, '921000 Housing Services', null, 36);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005180', null, '921200 Primary Housing Services', null, 37);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005181', null, '921300 Other Housing Services', null, 38);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005182', null, '922000 Travel & Transport Services', null, 39);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005183', null, '922100 Services for Personal Transport Equipment', null, 40);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005184', null, '922200 Transport Services', null, 41);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005185', null, '924000 Recreation & Personal Services', null, 42);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005186', null, '924100 Package Holidays & Accommodation', null, 43);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005187', null, '924200 Other Recreational & Personal Services', null, 44);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005188', null, '924210 Catering Services', null, 45);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005189', null, '924220 Non-Catering Recreational & Personal Services', null, 46);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005190', null, '925000 Miscellaneous and Other Services', null, 47);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005191', null, '925200 Miscellaneous Services', null, 48);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005192', null, '931000 Non-Seasonal Food', null, 50);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005193', null, '932000 Durables', null, 51);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005194', null, '933000 Semi-Durables', null, 52);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005195', null, '934000 Non-Durables', null, 53);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005196', null, '935000 Energy, Food, Alcohol & Tobacco', null, 54);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005197', null, '936000 Energy & Non-processed Food', null, 55);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005198', null, '937000 Energy & Seasonal Food', null, 56);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005199', null, '938000 Education, Health & Social Protection', null, 57);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005200', null, '941000 CPI excluding Energy', null, 58);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005201', null, '942000 CPI excluding Energy, Food, Alcohol & Tobacco', null, 59);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005202', null, '943000 CPI excluding Seasonal Food', null, 60);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005203', null, '944000 CPI excluding Liquid Fuels, Vehicle Fuels & Lubrication', null, 61);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005204', null, '945000 CPI excluding Energy & Seasonal Food', null, 62);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005205', null, '946000 CPI excluding Energy & Non-processed Food', null, 63);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005206', null, '947000 CPI excluding Alcohol & Tobacco', null, 64);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005207', null, '948000 CPI excluding Housing, Water, Electricity, Gas &', null, 65);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005208', null, '949000 CPI excluding Education, Health & Social Protection', null, 66);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005209', null, '951000 CPI excluding tobacco', null, 67);
insert into hierarchy_entry (hierarchy_id, code, parent_code, name, hierarchy_level_type_id, display_order) values ('CL_0000641', 'CI_0005212', null, '925300 Medical Services', null, 49);
