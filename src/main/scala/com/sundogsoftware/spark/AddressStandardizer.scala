package com.sundogsoftware.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import StringCleaner._

import scala.util.matching.Regex

object AddressStandardizer {

  private val patternsC1: Seq[(Regex, String)] = Seq(
    "\\bALLEE\\b".r -> "ALY",
    "\\bALLEY\\b".r -> "ALY",
    "\\bALLY\\b".r -> "ALY",
    "\\bALY\\b".r -> "ALY",
    "\\bANEX\\b".r -> "ANX",
    "\\bANNEX\\b".r -> "ANX",
    "\\bANNX\\b".r -> "ANX",
    "\\bANX\\b".r -> "ANX",
    "\\bARC\\b".r -> "ARC",
    "\\bARCADE\\b".r -> "ARC",
    "\\bAV\\b".r -> "AVE",
    "\\bAVE\\b".r -> "AVE",
    "\\bAVEN\\b".r -> "AVE",
    "\\bAVENU\\b".r -> "AVE",
    "\\bAVENUE\\b".r -> "AVE",
    "\\bAVN\\b".r -> "AVE",
    "\\bAVNUE\\b".r -> "AVE",
    "\\bBAYOO\\b".r -> "BAYOU",
    "\\bBAYOU\\b".r -> "BAYOU",
    "\\bBYU\\b".r -> "BAYOU",
    "\\bBCH\\b".r -> "BCH",
    "\\bBEACH\\b".r -> "BCH",
    "\\bBEND\\b".r -> "BND",
    "\\bBND\\b".r -> "BND",
    "\\bBLF\\b".r -> "BLF",
    "\\bBLUF\\b".r -> "BLF",
    "\\bBLUFF\\b".r -> "BLF",
    "\\bBLUFFS\\b".r -> "BLFS",
    "\\bBLFS\\b".r -> "BLFS",
    "\\bBOT\\b".r -> "BTM",
    "\\bBTM\\b".r -> "BTM",
    "\\bBOTTM\\b".r -> "BTM",
    "\\bBOTTOM\\b".r -> "BTM",
    "\\bBLVD\\b".r -> "BLVD",
    "\\bBOUL\\b".r -> "BLVD",
    "\\bBOULEVARD\\b".r -> "BLVD",
    "\\bBOULV\\b".r -> "BLVD",
    "\\bBR\\b".r -> "BR",
    "\\bBRNCH\\b".r -> "BR",
    "\\bBRANCH\\b".r -> "BR",
    "\\bBRDGE\\b".r -> "BRG",
    "\\bBRG\\b".r -> "BRG",
    "\\bBRIDGE\\b".r -> "BRG",
    "\\bBRK\\b".r -> "BRK",
    "\\bBROOK\\b".r -> "BRK",
    "\\bBROOKS\\b".r -> "BRKS",
    "\\bBURG\\b".r -> "BG",
    "\\bBURGS\\b".r -> "BGS",
    "\\bBYP\\b".r -> "BYP",
    "\\bBYPA\\b".r -> "BYP",
    "\\bBYPAS\\b".r -> "BYP",
    "\\bBYPASS\\b".r -> "BYP",
    "\\bBYPS\\b".r -> "BYP",
    "\\bCAMP\\b".r -> "CP",
    "\\bCP\\b".r -> "CP",
    "\\bCMP\\b".r -> "CP",
    "\\bCANYN\\b".r -> "CYN",
    "\\bCANYON\\b".r -> "CYN",
    "\\bCNYN\\b".r -> "CYN",
    "\\bCAPE\\b".r -> "CPE",
    "\\bCPE\\b".r -> "CPE",
    "\\bCAUSEWAY\\b".r -> "CSWY",
    "\\bCAUSWA\\b".r -> "CSWY",
    "\\bCSWY\\b".r -> "CSWY",
    "\\bCEN\\b".r -> "CTR",
    "\\bCENT\\b".r -> "CTR",
    "\\bCENTER\\b".r -> "CTR",
    "\\bCENTR\\b".r -> "CTR",
    "\\bCENTRE\\b".r -> "CTR",
    "\\bCNTER\\b".r -> "CTR",
    "\\bCNTR\\b".r -> "CTR",
    "\\bCTR\\b".r -> "CTR",
    "\\bCENTERS\\b".r -> "CTRS",
    "\\bCIR\\b".r -> "CIR",
    "\\bCIRC\\b".r -> "CIR",
    "\\bCIRCL\\b".r -> "CIR",
    "\\bCIRCLE\\b".r -> "CIR",
    "\\bCRCL\\b".r -> "CIR",
    "\\bCRCLE\\b".r -> "CIR",
    "\\bCIRCLES\\b".r -> "CIRS",
    "\\bCLF\\b".r -> "CLF",
    "\\bCLIFF\\b".r -> "CLF",
    "\\bCLFS\\b".r -> "CLFS",
    "\\bCLIFFS\\b".r -> "CLFS",
    "\\bCLB\\b".r -> "CLB",
    "\\bCLUB\\b".r -> "CLB",
    "\\bCOMMON\\b".r -> "CMN",
    "\\bCOMMONS\\b".r -> "CMNS",
    "\\bCOR\\b".r -> "COR",
    "\\bCORNER\\b".r -> "COR",
    "\\bCORNERS\\b".r -> "CORS",
    "\\bCORS\\b".r -> "CORS",
    "\\bCOURSE\\b".r -> "CRSE",
    "\\bCRSE\\b".r -> "CRSE",
    "\\bCOURT\\b".r -> "CT",
    "\\bCT\\b".r -> "CT",
    "\\bCOURTS\\b".r -> "CTS",
    "\\bCTS\\b".r -> "CTS",
    "\\bCOVE\\b".r -> "CV",
    "\\bCV\\b".r -> "CV",
    "\\bCOVES\\b".r -> "CVS",
    "\\bCREEK\\b".r -> "CRK",
    "\\bCRK\\b".r -> "CRK",
    "\\bCRESCENT\\b".r -> "CRES",
    "\\bCRES\\b".r -> "CRES",
    "\\bCRSENT\\b".r -> "CRES",
    "\\bCRSNT\\b".r -> "CRES",
    "\\bCREST\\b".r -> "CRST",
    "\\bCROSSING\\b".r -> "XING",
    "\\bCRSSNG\\b".r -> "XING",
    "\\bXING\\b".r -> "XING",
    "\\bCROSSROAD\\b".r -> "XRD",
    "\\bCROSSROADS\\b".r -> "XRDS",
    "\\bCURVE\\b".r -> "CURV",
    "\\bDALE\\b".r -> "DL",
    "\\bDL\\b".r -> "DL",
    "\\bDAM\\b".r -> "DM",
    "\\bDM\\b".r -> "DM",
    "\\bDIV\\b".r -> "DV",
    "\\bDIVIDE\\b".r -> "DV",
    "\\bDV\\b".r -> "DV",
    "\\bDVD\\b".r -> "DV",
    "\\bDR\\b".r -> "DR",
    "\\bDRIV\\b".r -> "DR",
    "\\bDRIVE\\b".r -> "DR",
    "\\bDRV\\b".r -> "DR",
    "\\bDRIVES\\b".r -> "DRS",
    "\\bEST\\b".r -> "EST",
    "\\bESTATE\\b".r -> "EST",
    "\\bESTATES\\b".r -> "ESTS",
    "\\bESTS\\b".r -> "ESTS",
    "\\bEXP\\b".r -> "EXPY",
    "\\bEXPR\\b".r -> "EXPY",
    "\\bEXPRESS\\b".r -> "EXPY",
    "\\bEXPRESSWAY\\b".r -> "EXPY",
    "\\bEXPW\\b".r -> "EXPY",
    "\\bEXPY\\b".r -> "EXPY",
    "\\bEXT\\b".r -> "EXT",
    "\\bEXTENSION\\b".r -> "EXT",
    "\\bEXTN\\b".r -> "EXT",
    "\\bEXTNSN\\b".r -> "EXT",
    "\\bEXTS\\b".r -> "EXTS",
    "\\bFALL\\b".r -> "FALL",
    "\\bFALLS\\b".r -> "FLS",
    "\\bFLS\\b".r -> "FLS",
    "\\bFERRY\\b".r -> "FRY",
    "\\bFRRY\\b".r -> "FRY",
    "\\bFRY\\b".r -> "FRY",
    "\\bFIELD\\b".r -> "FLD",
    "\\bFLD\\b".r -> "FLD",
    "\\bFIELDS\\b".r -> "FLDS",
    "\\bFLDS\\b".r -> "FLDS",
    "\\bFLAT\\b".r -> "FLT",
    "\\bFLT\\b".r -> "FLT",
    "\\bFLATS\\b".r -> "FLTS",
    "\\bFLTS\\b".r -> "FLTS",
    "\\bFORD\\b".r -> "FRD",
    "\\bFRD\\b".r -> "FRD",
    "\\bFORDS\\b".r -> "FRDS",
    "\\bFOREST\\b".r -> "FRST",
    "\\bFORESTS\\b".r -> "FRST",
    "\\bFRST\\b".r -> "FRST",
    "\\bFORG\\b".r -> "FRG",
    "\\bFORGE\\b".r -> "FRG",
    "\\bFRG\\b".r -> "FRG",
    "\\bFORGES\\b".r -> "FRGS",
    "\\bFORK\\b".r -> "FRK",
    "\\bFRK\\b".r -> "FRK",
    "\\bFORKS\\b".r -> "FRKS",
    "\\bFRKS\\b".r -> "FRKS",
    "\\bFORT\\b".r -> "FT",
    "\\bFRT\\b".r -> "FT",
    "\\bFT\\b".r -> "FT",
    "\\bFREEWAY\\b".r -> "FWY",
    "\\bFREEWY\\b".r -> "FWY",
    "\\bFRWAY\\b".r -> "FWY",
    "\\bFRWY\\b".r -> "FWY",
    "\\bFWY\\b".r -> "FWY",
    "\\bGARDEN\\b".r -> "GDN",
    "\\bGARDN\\b".r -> "GDN",
    "\\bGRDEN\\b".r -> "GDN",
    "\\bGRDN\\b".r -> "GDN",
    "\\bGARDENS\\b".r -> "GDNS",
    "\\bGDNS\\b".r -> "GDNS",
    "\\bGRDNS\\b".r -> "GDNS",
    "\\bGATEWAY\\b".r -> "GTWY",
    "\\bGATEWY\\b".r -> "GTWY",
    "\\bGATWAY\\b".r -> "GTWY",
    "\\bGTWAY\\b".r -> "GTWY",
    "\\bGTWY\\b".r -> "GTWY",
    "\\bGLEN\\b".r -> "GLN",
    "\\bGLN\\b".r -> "GLN",
    "\\bGLENS\\b".r -> "GLNS",
    "\\bGREEN\\b".r -> "GRN",
    "\\bGRN\\b".r -> "GRN",
    "\\bGREENS\\b".r -> "GRNS",
    "\\bGROV\\b".r -> "GRV",
    "\\bGROVE\\b".r -> "GRV",
    "\\bGRV\\b".r -> "GRV",
    "\\bGROVES\\b".r -> "GRVS",
    "\\bHARB\\b".r -> "HBR",
    "\\bHARBOR\\b".r -> "HBR",
    "\\bHARBR\\b".r -> "HBR",
    "\\bHBR\\b".r -> "HBR",
    "\\bHRBOR\\b".r -> "HBR",
    "\\bHARBORS\\b".r -> "HBRS",
    "\\bHAVEN\\b".r -> "HVN",
    "\\bHVN\\b".r -> "HVN",
    "\\bHT\\b".r -> "HTS",
    "\\bHEIGHTS\\b".r -> "HTS",
    "\\bHTS\\b".r -> "HTS",
    "\\bHIGHWAY\\b".r -> "HWY",
    "\\bHIGHWY\\b".r -> "HWY",
    "\\bHIWAY\\b".r -> "HWY",
    "\\bHIWY\\b".r -> "HWY",
    "\\bHWAY\\b".r -> "HWY",
    "\\bHWY\\b".r -> "HWY",
    "\\bHILL\\b".r -> "HL",
    "\\bHL\\b".r -> "HL",
    "\\bHILLS\\b".r -> "HLS",
    "\\bHLS\\b".r -> "HLS",
    "\\bHLLW\\b".r -> "HOLW",
    "\\bHOLLOW\\b".r -> "HOLW",
    "\\bHOLLOWS\\b".r -> "HOLW",
    "\\bHOLW\\b".r -> "HOLW",
    "\\bHOLWS\\b".r -> "HOLW",
    "\\bINLT\\b".r -> "INLT",
    "\\bINLET\\b".r -> "INLT",
    "\\bIS\\b".r -> "IS",
    "\\bISLAND\\b".r -> "IS",
    "\\bISLND\\b".r -> "IS",
    "\\bISLANDS\\b".r -> "ISS",
    "\\bISLNDS\\b".r -> "ISS",
    "\\bISS\\b".r -> "ISS",
    "\\bISLE\\b".r -> "ISLE",
    "\\bISLES\\b".r -> "ISLE",
    "\\bJCT\\b".r -> "JCT",
    "\\bJCTION\\b".r -> "JCT",
    "\\bJCTN\\b".r -> "JCT",
    "\\bJUNCTION\\b".r -> "JCT",
    "\\bJUNCTN\\b".r -> "JCT",
    "\\bJUNCTON\\b".r -> "JCT",
    "\\bJCTNS\\b".r -> "JCTS",
    "\\bJCTS\\b".r -> "JCTS",
    "\\bJUNCTIONS\\b".r -> "JCTS",
    "\\bKEY\\b".r -> "KY",
    "\\bKY\\b".r -> "KY",
    "\\bKEYS\\b".r -> "KYS",
    "\\bKYS\\b".r -> "KYS",
    "\\bKNL\\b".r -> "KNL",
    "\\bKNOL\\b".r -> "KNL",
    "\\bKNOLL\\b".r -> "KNL",
    "\\bKNLS\\b".r -> "KNLS",
    "\\bKNOLLS\\b".r -> "KNLS",
    "\\bLK\\b".r -> "LK",
    "\\bLAKE\\b".r -> "LK",
    "\\bLKS\\b".r -> "LKS",
    "\\bLAKES\\b".r -> "LKS",
    "\\bLAND\\b".r -> "LAND",
    "\\bLANDING\\b".r -> "LNDG",
    "\\bLNDG\\b".r -> "LNDG",
    "\\bLNDNG\\b".r -> "LNDG",
    "\\bLANE\\b".r -> "LN",
    "\\bLN\\b".r -> "LN",
    "\\bLGT\\b".r -> "LGT",
    "\\bLIGHT\\b".r -> "LGT",
    "\\bLIGHTS\\b".r -> "LGTS",
    "\\bLF\\b".r -> "LF",
    "\\bLOAF\\b".r -> "LF",
    "\\bLCK\\b".r -> "LCK",
    "\\bLOCK\\b".r -> "LCK",
    "\\bLCKS\\b".r -> "LCKS",
    "\\bLOCKS\\b".r -> "LCKS",
    "\\bLDG\\b".r -> "LDG",
    "\\bLDGE\\b".r -> "LDG",
    "\\bLODG\\b".r -> "LDG",
    "\\bLODGE\\b".r -> "LDG",
    "\\bLOOP\\b".r -> "LOOP",
    "\\bLOOPS\\b".r -> "LOOP",
    "\\bMALL\\b".r -> "MALL",
    "\\bMNR\\b".r -> "MNR",
    "\\bMANOR\\b".r -> "MNR",
    "\\bMANORS\\b".r -> "MNRS",
    "\\bMNRS\\b".r -> "MNRS",
    "\\bMEADOW\\b".r -> "MDW",
    "\\bMDW\\b".r -> "MDWS",
    "\\bMDWS\\b".r -> "MDWS",
    "\\bMEADOWS\\b".r -> "MDWS",
    "\\bMEDOWS\\b".r -> "MDWS",
    "\\bMEWS\\b".r -> "MEWS",
    "\\bMILL\\b".r -> "ML",
    "\\bMILLS\\b".r -> "MLS",
    "\\bMISSN\\b".r -> "MSN",
    "\\bMSSN\\b".r -> "MSN",
    "\\bMOTORWAY\\b".r -> "MTWY",
    "\\bMNT\\b".r -> "MT",
    "\\bMT\\b".r -> "MT",
    "\\bMOUNT\\b".r -> "MT",
    "\\bMNTAIN\\b".r -> "MTN",
    "\\bMNTN\\b".r -> "MTN",
    "\\bMOUNTAIN\\b".r -> "MTN",
    "\\bMOUNTIN\\b".r -> "MTN",
    "\\bMTIN\\b".r -> "MTN",
    "\\bMTN\\b".r -> "MTN",
    "\\bMNTNS\\b".r -> "MTNS",
    "\\bMOUNTAINS\\b".r -> "MTNS",
    "\\bNCK\\b".r -> "NCK",
    "\\bNECK\\b".r -> "NCK",
    "\\bORCH\\b".r -> "ORCH",
    "\\bORCHARD\\b".r -> "ORCH",
    "\\bORCHRD\\b".r -> "ORCH",
    "\\bOVAL\\b".r -> "OVAL",
    "\\bOVL\\b".r -> "OVAL",
    "\\bOVERPASS\\b".r -> "OPAS",
    "\\bPARK\\b".r -> "PARK",
    "\\bPRK\\b".r -> "PARK",
    "\\bPARKS\\b".r -> "PARK",
    "\\bPARKWAY\\b".r -> "PKWY",
    "\\bPARKWY\\b".r -> "PKWY",
    "\\bPKWAY\\b".r -> "PKWY",
    "\\bPKWY\\b".r -> "PKWY",
    "\\bPKY\\b".r -> "PKWY",
    "\\bPARKWAYS\\b".r -> "PKWY",
    "\\bPKWYS\\b".r -> "PKWY",
    "\\bPASS\\b".r -> "PASS",
    "\\bPASSAGE\\b".r -> "PSGE",
    "\\bPATH\\b".r -> "PATH",
    "\\bPATHS\\b".r -> "PATH",
    "\\bPIKE\\b".r -> "PIKE",
    "\\bPIKES\\b".r -> "PIKE",
    "\\bPINE\\b".r -> "PNE",
    "\\bPINES\\b".r -> "PNES",
    "\\bPNES\\b".r -> "PNES",
    "\\bPL\\b".r -> "PL",
    "\\bPLACE\\b".r -> "PL",
    "\\bPLAIN\\b".r -> "PLN",
    "\\bPLN\\b".r -> "PLN",
    "\\bPLAINS\\b".r -> "PLNS",
    "\\bPLNS\\b".r -> "PLNS",
    "\\bPLAZA\\b".r -> "PLZ",
    "\\bPLZ\\b".r -> "PLZ",
    "\\bPLZA\\b".r -> "PLZ",
    "\\bPOINT\\b".r -> "PT",
    "\\bPT\\b".r -> "PT",
    "\\bPOINTS\\b".r -> "PTS",
    "\\bPTS\\b".r -> "PTS",
    "\\bPORT\\b".r -> "PRT",
    "\\bPRT\\b".r -> "PRT",
    "\\bPORTS\\b".r -> "PRTS",
    "\\bPRTS\\b".r -> "PRTS",
    "\\bPR\\b".r -> "PR",
    "\\bPRAIRIE\\b".r -> "PR",
    "\\bPRR\\b".r -> "PR",
    "\\bRAD\\b".r -> "RADL",
    "\\bRADIAL\\b".r -> "RADL",
    "\\bRADIEL\\b".r -> "RADL",
    "\\bRADL\\b".r -> "RADL",
    "\\bRAMP\\b".r -> "RAMP",
    "\\bRANCH\\b".r -> "RNCH",
    "\\bRANCHES\\b".r -> "RNCH",
    "\\bRNCH\\b".r -> "RNCH",
    "\\bRNCHS\\b".r -> "RNCH",
    "\\bRAPID\\b".r -> "RPD",
    "\\bRPD\\b".r -> "RPD",
    "\\bRAPIDS\\b".r -> "RPDS",
    "\\bRPDS\\b".r -> "RPDS",
    "\\bREST\\b".r -> "RST",
    "\\bRST\\b".r -> "RST",
    "\\bRDG\\b".r -> "RDG",
    "\\bRDGE\\b".r -> "RDG",
    "\\bRIDGE\\b".r -> "RDG",
    "\\bRDGS\\b".r -> "RDGS",
    "\\bRIDGES\\b".r -> "RDGS",
    "\\bRIV\\b".r -> "RIV",
    "\\bRIVER\\b".r -> "RIV",
    "\\bRVR\\b".r -> "RIV",
    "\\bRIVR\\b".r -> "RIV",
    "\\bRD\\b".r -> "RD",
    "\\bROAD\\b".r -> "RD",
    "\\bROADS\\b".r -> "RDS",
    "\\bRDS\\b".r -> "RDS",
    "\\bROUTE\\b".r -> "RTE",
    "\\bROW\\b".r -> "ROW",
    "\\bRUE\\b".r -> "RUE",
    "\\bRUN\\b".r -> "RUN",
    "\\bSHL\\b".r -> "SHL",
    "\\bSHOAL\\b".r -> "SHL",
    "\\bSHLS\\b".r -> "SHLS",
    "\\bSHOALS\\b".r -> "SHLS",
    "\\bSHOAR\\b".r -> "SHR",
    "\\bSHORE\\b".r -> "SHR",
    "\\bSHR\\b".r -> "SHR",
    "\\bSHOARS\\b".r -> "SHRS",
    "\\bSHORES\\b".r -> "SHRS",
    "\\bSHRS\\b".r -> "SHRS",
    "\\bSKYWAY\\b".r -> "SKWY",
    "\\bSPG\\b".r -> "SPG",
    "\\bSPNG\\b".r -> "SPG",
    "\\bSPRING\\b".r -> "SPG",
    "\\bSPRNG\\b".r -> "SPG",
    "\\bSPGS\\b".r -> "SPGS",
    "\\bSPNGS\\b".r -> "SPGS",
    "\\bSPRINGS\\b".r -> "SPGS",
    "\\bSPRNGS\\b".r -> "SPGS",
    "\\bSPUR\\b".r -> "SPUR",
    "\\bSPURS\\b".r -> "SPUR",
    "\\bSQ\\b".r -> "SQ",
    "\\bSQR\\b".r -> "SQ",
    "\\bSQRE\\b".r -> "SQ",
    "\\bSQU\\b".r -> "SQ",
    "\\bSQUARE\\b".r -> "SQ",
    "\\bSQRS\\b".r -> "SQS",
    "\\bSQUARES\\b".r -> "SQS",
    "\\bSTA\\b".r -> "STA",
    "\\bSTATION\\b".r -> "STA",
    "\\bSTATN\\b".r -> "STA",
    "\\bSTN\\b".r -> "STA",
    "\\bSTRA\\b".r -> "STRA",
    "\\bSTRAV\\b".r -> "STRA",
    "\\bSTRAVEN\\b".r -> "STRA",
    "\\bSTRAVENUE\\b".r -> "STRA",
    "\\bSTRAVN\\b".r -> "STRA",
    "\\bSTRVN\\b".r -> "STRA",
    "\\bSTRVNUE\\b".r -> "STRA",
    "\\bSTREAM\\b".r -> "STRM",
    "\\bSTREME\\b".r -> "STRM",
    "\\bSTRM\\b".r -> "STRM",
    "\\bSTREET\\b".r -> "ST",
    "\\bSTRT\\b".r -> "ST",
    "\\bST\\b".r -> "ST",
    "\\bSTR\\b".r -> "ST",
    "\\bSTREETS\\b".r -> "STS",
    "\\bSMT\\b".r -> "SMT",
    "\\bSUMIT\\b".r -> "SMT",
    "\\bSUMITT\\b".r -> "SMT",
    "\\bSUMMIT\\b".r -> "SMT",
    "\\bTER\\b".r -> "TER",
    "\\bTERR\\b".r -> "TER",
    "\\bTERRACE\\b".r -> "TER",
    "\\bTHROUGHWAY\\b".r -> "TRWY",
    "\\bTRACE\\b".r -> "TRCE",
    "\\bTRACES\\b".r -> "TRCE",
    "\\bTRCE\\b".r -> "TRCE",
    "\\bTRACK\\b".r -> "TRAK",
    "\\bTRACKS\\b".r -> "TRAK",
    "\\bTRAK\\b".r -> "TRAK",
    "\\bTRK\\b".r -> "TRAK",
    "\\bTRKS\\b".r -> "TRAK",
    "\\bTRAFFICWAY\\b".r -> "TRFY",
    "\\bTRAIL\\b".r -> "TRL",
    "\\bTRAILS\\b".r -> "TRL",
    "\\bTRL\\b".r -> "TRL",
    "\\bTRLS\\b".r -> "TRL",
    "\\bTRAILER\\b".r -> "TRLR",
    "\\bTRLR\\b".r -> "TRLR",
    "\\bTRLRS\\b".r -> "TRLR",
    "\\bTUNEL\\b".r -> "TUNL",
    "\\bTUNL\\b".r -> "TUNL",
    "\\bTUNLS\\b".r -> "TUNL",
    "\\bTUNNEL\\b".r -> "TUNL",
    "\\bTUNNELS\\b".r -> "TUNL",
    "\\bTUNNL\\b".r -> "TUNL",
    "\\bTRNPK\\b".r -> "TPKE",
    "\\bTURNPIKE\\b".r -> "TPKE",
    "\\bTURNPK\\b".r -> "TPKE",
    "\\bUNDERPASS\\b".r -> "UPAS",
    "\\bUN\\b".r -> "UN",
    "\\bUNION\\b".r -> "UN",
    "\\bUNIONS\\b".r -> "UNS",
    "\\bVALLEY\\b".r -> "VLY",
    "\\bVALLY\\b".r -> "VLY",
    "\\bVLLY\\b".r -> "VLY",
    "\\bVLY\\b".r -> "VLY",
    "\\bVALLEYS\\b".r -> "VLYS",
    "\\bVLYS\\b".r -> "VLYS",
    "\\bVDCT\\b".r -> "VIA",
    "\\bVIA\\b".r -> "VIA",
    "\\bVIADCT\\b".r -> "VIA",
    "\\bVIADUCT\\b".r -> "VIA",
    "\\bVIEW\\b".r -> "VW",
    "\\bVW\\b".r -> "VW",
    "\\bVIEWS\\b".r -> "VWS",
    "\\bVWS\\b".r -> "VWS",
    "\\bVILL\\b".r -> "VLG",
    "\\bVILLAG\\b".r -> "VLG",
    "\\bVILLAGE\\b".r -> "VLG",
    "\\bVILLG\\b".r -> "VLG",
    "\\bVILLIAGE\\b".r -> "VLG",
    "\\bVLG\\b".r -> "VLG",
    "\\bVILLAGES\\b".r -> "VLGS",
    "\\bVLGS\\b".r -> "VLGS",
    "\\bVILLE\\b".r -> "VL",
    "\\bVL\\b".r -> "VL",
    "\\bVIS\\b".r -> "VIS",
    "\\bVIST\\b".r -> "VIS",
    "\\bVISTA\\b".r -> "VIS",
    "\\bVST\\b".r -> "VIS",
    "\\bVSTA\\b".r -> "VIS",
    "\\bWALK\\b".r -> "WALK",
    "\\bWALKS\\b".r -> "WALK",
    "\\bWALL\\b".r -> "WALL",
    "\\bWY\\b".r -> "WAY",
    "\\bWAY\\b".r -> "WAY",
    "\\bWAYS\\b".r -> "WAYS",
    "\\bWELL\\b".r -> "WL",
    "\\bWELLS\\b".r -> "WLS",
    "\\bWLS\\b".r -> "WLS",
  )

  private val patternsC2: Seq[(Regex, String)] = Seq(
    "\\bAPARTMENT\\b".r -> "APT",
    "\\bBASEMENT\\b".r -> "BSMT",
    "\\bBUILDING\\b".r -> "BLDG",
    "\\bDEPARTMENT\\b".r -> "DEPT",
    "\\bFLOOR\\b".r -> "FL",
    "\\bFRONT\\b".r -> "FRNT",
    "\\bHANGER\\b".r -> "HNGR",
    "\\bKEY\\b".r -> "KEY",
    "\\bLOBBY\\b".r -> "LBBY",
    "\\bLOT\\b".r -> "LOT",
    "\\bLOWER\\b".r -> "LOWR",
    "\\bOFFICE\\b".r -> "OFC",
    "\\bPENTHOUSE\\b".r -> "PH",
    "\\bPIER\\b".r -> "PIER",
    "\\bREAR\\b".r -> "REAR",
    "\\bROOM\\b".r -> "RM",
    "\\bSIDE\\b".r -> "SIDE",
    "\\bSLIP\\b".r -> "SLIP",
    "\\bSPACE\\b".r -> "SPC",
    "\\bSTOP\\b".r -> "STOP",
    "\\bSUITE\\b".r -> "STE",
    "\\bTRAILER\\b".r -> "TRLR",
    "\\bUNIT\\b".r -> "UNIT",
    "\\bUPPER\\b".r -> "UPPR",
  )

  private val patternsState: Seq[(Regex, String)] = Seq(
    "\\bALABAMA\\b".r -> "AL",
    "\\bALASKA\\b".r -> "AK",
    "\\bARIZONA\\b".r -> "AZ",
    "\\bARKANSAS\\b".r -> "AR",
    "\\bCALIFORNIA\\b".r -> "CA",
    "\\bCOLORADO\\b".r -> "CO",
    "\\bCONNECTICUT\\b".r -> "CT",
    "\\bDELAWARE\\b".r -> "DE",
    "\\bFLORIDA\\b".r -> "FL",
    "\\bGEORGIA\\b".r -> "GA",
    "\\bHAWAII\\b".r -> "HI",
    "\\bIDAHO\\b".r -> "ID",
    "\\bILLINOIS\\b".r -> "IL",
    "\\bINDIANA\\b".r -> "IN",
    "\\bIOWA\\b".r -> "IA",
    "\\bKANSAS\\b".r -> "KS",
    "\\bKENTUCKY\\b".r -> "KY",
    "\\bLOUISIANA\\b".r -> "LA",
    "\\bMAINE\\b".r -> "ME",
    "\\bMARYLAND\\b".r -> "MD",
    "\\bMASSACHUSETTS\\b".r -> "MA",
    "\\bMICHIGAN\\b".r -> "MI",
    "\\bMINNESOTA\\b".r -> "MN",
    "\\bMISSISSIPPI\\b".r -> "MS",
    "\\bMISSOURI\\b".r -> "MO",
    "\\bMONTANA\\b".r -> "MT",
    "\\bNEBRASKA\\b".r -> "NE",
    "\\bNEVADA\\b".r -> "NV",
    "\\bNEW HAMPSHIRE\\b".r -> "NH",
    "\\bNEW JERSEY\\b".r -> "NJ",
    "\\bNEW MEXICO\\b".r -> "NM",
    "\\bNEW YORK\\b".r -> "NY",
    "\\bNORTH CAROLINA\\b".r -> "NC",
    "\\bNORTH DAKOTA\\b".r -> "ND",
    "\\bOHIO\\b".r -> "OH",
    "\\bOKLAHOMA\\b".r -> "OK",
    "\\bOREGON\\b".r -> "OR",
    "\\bPENNSYLVANIA\\b".r -> "PA",
    "\\bRHODE ISLAND\\b".r -> "RI",
    "\\bSOUTH CAROLINA\\b".r -> "SC",
    "\\bSOUTH DAKOTA\\b".r -> "SD",
    "\\bTENNESSEE\\b".r -> "TN",
    "\\bTEXAS\\b".r -> "TX",
    "\\bUTAH\\b".r -> "UT",
    "\\bVERMONT\\b".r -> "VT",
    "\\bVIRGINIA\\b".r -> "VA",
    "\\bWASHINGTON\\b".r -> "WA",
    "\\bWEST VIRGINIA\\b".r -> "WV",
    "\\bWISCONSIN\\b".r -> "WI",
    "\\bWYOMING\\b".r -> "WY",
  )

  var standardizeAddress: UserDefinedFunction = udf((input: String) => {
    if (input == null) null
    else {
        var address = cleanString(input)
      address = standardizeAddressC1(address)
      address = standardizeAddressC2(address)
      address = standardizeState(address)

      address
    }
  })

  private val standardizeAddressC1 = (input: String) => {
    if (input == null) null
    else {
      var address = input.toUpperCase
      for ((pattern, replacement) <- patternsC1) {
        address = pattern.replaceAllIn(address, replacement)
      }
      address
    }
  }

  private val standardizeAddressC2: String => String = (input: String) => {
    if (input == null) null
    else {
      var address = input.toUpperCase
      for ((pattern, replacement) <- patternsC2) {
        address = pattern.replaceAllIn(address, replacement)
      }
      address
    }
  }

  private val standardizeState: String => String = (input: String) => {
    if (input == null) null
    else {
      var address = input.toUpperCase
      for ((pattern, replacement) <- patternsState) {
        address = pattern.replaceAllIn(address, replacement)
      }
      address
    }
  }

}