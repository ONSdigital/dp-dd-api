package utils;

public class GeogHelper {

	/*
	ABBREVIATION	PREFIX [E]	PREFIX [W]	DESCRIPTION								MAP FOLDER	THEME			COVERAGE
	BUA				E34			W37			Built-up Areas							BUA			Census			England and Wales
	BUASD			E35			W38			Built-up Area sub-divisions				BUASD		Census			England and Wales
	CCG				E38			-			Clinical Commissioning Groups			CCG			Health			England
	CMCTY			E42			-			Census Merged Counties					CMCTY		Census			England
	CMLAD			E41			W40			Census Merged Local Authority Districts	CMLAD		Census			England and Wales
	CMWD			E36			W39			Census Merged Wards						CMWD		Census			England and Wales
	COM				-			W04			Communities								PAR			Administrative	Wales
	CTRY			E92			E92			Country									COM			Administrative	United Kingdom
	CTY				E10			-			Counties								LMCTYUA		Administrative	England
	IOL				E13			-			Inner and Outer London					LMCTYUA		Administrative	England
	LHB				-			W11			Local Health Boards						LHB			Health			Wales
	LONB			E09			-			London Boroughs							LAD			Administrative	England
	LSOA			E01			W01			Super Output Areas, Lower Layer			LSOA		Statistical Building Block	England and Wales
	MCTY			E11			-			Metropolitan Counties					LMCTYUA		Administrative	England
	MD				E08			-			Metropolitan Districts					LAD			Administrative	England
	MSOA			E02			W02			Super Output Areas, Middle Layer		MSOA		Statistical Building Block	England and Wales
	NAWC			-			W09			National Assembly Wales Constituencies	NAWC		Electoral		Wales
	NAWER			-			W10			National Assembly Wales Electrl Regions	NAWER		Electoral		Wales
	NHSAT			E39			-			NHS Commissioning Area Teams			NHSAT		Health	England
	NHSCR			E40			-			NHS Commissioning Regions				NHSCR		Health	England
	NMD				E07			-			Non-metropolitan Districts				LAD			Administrative	England
	NPARK			E26			W18			National Parks							NPARK		Other			Great Britain
	OA				E00			W00			Output Areas							OA			Statistical Building Block	United Kingdom
	PAR				E04			-			Civil Parishes							PAR			Administrative	England
	PCT				E16			-			Primary Care Trusts						PCO			Health			England
	RGN				E12			-			Regions									GOR			Administrative	England
	SHA				E18			-			Strategic Health Authorities			SHA			Health			England
	UA				E06			W06			Unitary Authorities						LAD			Administrative	England and Wales
	WD				E05			W05			Electoral Wards/Divisions				WD			Administrative/Electoral	England and Wales
	WPC				E14			W07			Westminster Parliamentary Constituency	PCON		Electoral		United Kingdom
	WZ				E33			W35			Workplace Zones							WZ			Census			England, Wales
	*/
	
public static String getLevelTypeFromAbb(String abb){

	if (abb.equals("K02")) {
	return "UK";
}
	if (abb.equals("K03")) {
	return "GB";
}
	if (abb.equals("K04")) {
	return "NAT";	
}
	if (abb.equals("E92")) {
	return "CTRY";	
}
	if (abb.equals("W92")) {
		return "CTRY";	
}
	if (abb.equals("N92")) {
		return "CTRY";	
}
	if (abb.equals("S92")) {
		return "CTRY";	
}
	if (abb.equals("E12")) {
		return "GOR";	
}
	if (abb.equals("E06")) {
		return "LAD";	
}
	if (abb.equals("E07")) {
		return "LAD";	
}
	if (abb.equals("E08")) {
		return "LAD";	
}
	if (abb.equals("E09")) {
		return "LAD";	
}
	return "NOT FOUND";
}
public static String getAreaTypeFromAbb(String abb){
	if (abb.equals("K02")) {
	return "UK";
}
	if (abb.equals("K03")) {
	return "GB";
}
	if (abb.equals("K04")) {
	return "NAT";	
}
	if (abb.equals("E92")) {
	return "CTRY";	
}
	if (abb.equals("W92")) {
		return "CTRY";	
}
	if (abb.equals("N92")) {
		return "CTRY";	
}
	if (abb.equals("S92")) {
		return "CTRY";	
}
	if (abb.equals("E12")) {
		return "GOR";	
}
	if (abb.equals("E06")) {
		return "UA";	
}
	if (abb.equals("E07")) {
		return "NMD";	
}
	if (abb.equals("E08")) {
		return "MD";	
}
	if (abb.equals("E09")) {
		return "LONB";	
}
	return "NOT FOUND";
}

public static String getNameFromExtcode(String extcode){
	
	return "United Kingdom";
}

}
