
#Add-Type -Path "c:\jobs\OracleDB-OCI\Oracle.ManagedDataAccess.dll"
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$dllPath = Join-Path $scriptPath "Oracle.ManagedDataAccess.dll"
#Add-Type -Path $dllPath

# === Config ===
$oci_host     = "172.16.201.66"
$port         = "1521"
$serviceName  = "NPODB01"
$user         = "JDERO"
$password     = "QgY8`$sBxk_27z"
$targetSchema = "SIDTA"
$timestamp    = Get-Date -Format "yyyyMMdd_HHmmss"
$dataFilesPath = Join-Path $scriptPath "DATA_FILES_TO_IMPORT"
$logFile = Join-Path $dataFilesPath "extraction.log"

function Get-JDEJulianFromDate {
    param (
        [datetime]$date
    )
    $year = $date.Year
    $dayOfYear = $date.DayOfYear
    $jdeYear = ($year - 1900)
    return [int]("$jdeYear$('{0:D3}' -f $dayOfYear)")
}

$jdeJulianToday = Get-JDEJulianFromDate -date (Get-Date)

# === Query Definitions ===
$queries = @{
    # Item Master
    F4101 = @{
        Sql     = "SELECT IMITM, IMLITM, IMAITM, IMDSC1, IMDSC2, IMSRTX, IMALN, IMSRP1, IMSRP2, IMSRP3, IMSRP4, IMSRP5, IMSRP6, IMSRP7, IMSRP8, IMSRP9, IMSRP0, IMPRP1, IMPRP2, IMPRP3, IMPRP4, IMPRP5, IMPRP6, IMPRP7, IMPRP8, IMPRP9, IMPRP0, IMCDCD, IMPDGR, IMDSGP, IMPRGR, IMRPRC, IMORPR, IMBUYR, IMDRAW, IMRVNO, IMDSZE, IMVCUD, IMCARS, IMCARP, IMSHCN, IMSHCM, IMUOM1, IMUOM2, IMUOM3, IMUOM4, IMUOM6, IMUOM8, IMUOM9, IMUWUM, IMUVM1, IMSUTM, IMUMVW, IMCYCL, IMGLPT, IMPLEV, IMPPLV, IMCLEV, IMPRPO, IMCKAV, IMBPFG, IMSRCE, IMOT1Y, IMOT2Y, IMSTDP, IMFRMP, IMTHRP, IMSTDG, IMFRGD, IMTHGD, IMCOTY, IMSTKT, IMLNTY, IMCONT, IMBACK, IMIFLA, IMTFLA, IMINMG, IMABCS, IMABCM, IMABCI, IMOVR, IMWARR, IMCMCG, IMSRNR, IMPMTH, IMFIFO, IMLOTS, IMSLD, IMANPL, IMMPST, IMPCTM, IMMMPC, IMPTSC, IMSNS, IMLTLV, IMLTMF, IMLTCM, IMOPC, IMOPV, IMACQ, IMMLQ, IMLTPU, IMMPSP, IMMRPP, IMITC, IMORDW, IMMTF1, IMMTF2, IMMTF3, IMMTF4, IMMTF5, IMEXPD, IMDEFD, IMSFLT, IMMAKE, IMCOBY, IMLLX, IMCMGL, IMCOMH, IMURCD, IMURDT, IMURAT, IMURAB, IMURRF, IMUSER, IMPID, IMJOBN, IMUPMJ, IMTDAY, IMUPCN, IMSCC0, IMUMUP, IMUMDF, IMUMS0, IMUMS1, IMUMS2, IMUMS3, IMUMS4, IMUMS5, IMUMS6, IMUMS7, IMUMS8, IMPOC, IMAVRT, IMEQTY, IMWTRQ, IMTMPL, IMSEG1, IMSEG2, IMSEG3, IMSEG4, IMSEG5, IMSEG6, IMSEG7, IMSEG8, IMSEG9, IMSEG0, IMMIC, IMAING, IMBBDD, IMCMDM, IMLECM, IMLEDD, IMPEFD, IMSBDD, IMU1DD, IMU2DD, IMU3DD, IMU4DD, IMU5DD, IMDLTL, IMDPPO, IMDUAL, IMXDCK, IMLAF, IMLTFM, IMRWLA, IMLNPA, IMLOTC, IMAPSC, IMAUOM, IMCONB, IMGCMP, IMPRI1, IMPRI2, IMASHL, IMVMINV, IMCMETH, IMEXPI, IMOPTH, IMCUTH, IMUMTH, IMLMFG, IMLINE, IMDFTPCT, IMKBIT, IMDFENDITM, IMKANEXLL, IMSCPSELL, IMMOPTH, IMMCUTH, IMCUMTH FROM SIDTA.F4101 WHERE IMUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F4101"
    }
    # Item Branch
    F4102 = @{
        Sql     = "SELECT IBITM, IBLITM, IBAITM, IBMCU, IBSRP1, IBSRP2, IBSRP3, IBSRP4, IBSRP5, IBSRP6, IBSRP7, IBSRP8, IBSRP9, IBSRP0, IBPRP1, IBPRP2, IBPRP3, IBPRP4, IBPRP5, IBPRP6, IBPRP7, IBPRP8, IBPRP9, IBPRP0, IBCDCD, IBPDGR, IBDSGP, IBVEND, IBANPL, IBBUYR, IBGLPT, IBORIG, IBROPI, IBROQI, IBRQMX, IBRQMN, IBWOMO, IBSERV, IBSAFE, IBSLD, IBCKAV, IBSRCE, IBLOTS, IBOT1Y, IBOT2Y, IBSTDP, IBFRMP, IBTHRP, IBSTDG, IBFRGD, IBTHGD, IBCOTY, IBMMPC, IBPRGR, IBRPRC, IBORPR, IBBACK, IBIFLA, IBABCS, IBABCM, IBABCI, IBOVR, IBSHCM, IBCARS, IBCARP, IBSHCN, IBSTKT, IBLNTY, IBFIFO, IBCYCL, IBINMG, IBWARR, IBSRNR, IBPCTM, IBCMCG, IBFUF1, IBTX, IBTAX1, IBMPST, IBMRPD, IBMRPC, IBUPC, IBSNS, IBMERL, IBLTLV, IBLTMF, IBLTCM, IBOPC, IBOPV, IBACQ, IBMLQ, IBLTPU, IBMPSP, IBMRPP, IBITC, IBECO, IBECTY, IBECOD, IBMTF1, IBMTF2, IBMTF3, IBMTF4, IBMTF5, IBMOVD, IBQUED, IBSETL, IBSRNK, IBSRKF, IBTIMB, IBBQTY, IBORDW, IBEXPD, IBDEFD, IBMULT, IBSFLT, IBMAKE, IBLFDJ, IBLLX, IBCMGL, IBURCD, IBURDT, IBURAT, IBURAB, IBURRF, IBUSER, IBPID, IBJOBN, IBUPMJ, IBTDAY, IBTFLA, IBCOMH, IBAVRT, IBPOC, IBAING, IBBBDD, IBCMDM, IBLECM, IBLEDD, IBMLOT, IBPEFD, IBSBDD, IBU1DD, IBU2DD, IBU3DD, IBU4DD, IBU5DD, IBXDCK, IBLAF, IBLTFM, IBRWLA, IBLNPA, IBLOTC, IBAPSC, IBPRI1, IBPRI2, IBLTCV, IBASHL, IBOPTH, IBCUTH, IBUMTH, IBLMFG, IBLINE, IBDFTPCT, IBKBIT, IBDFENDITM, IBKANEXLL, IBSCPSELL, IBMOPTH, IBMCUTH, IBCUMTH FROM SIDTA.F4102 WHERE IBUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F4102"
    }
    # Cross Reference
    F4104 = @{
        Sql     = "SELECT IVAN8, IVXRT, IVITM, IVEXDJ, IVEFTJ, IVMCU, IVCITM, IVDSC1, IVDSC2, IVALN, IVLITM, IVAITM, IVURCD, IVURDT, IVURAT, IVURAB, IVURRF, IVUSER, IVPID, IVJOBN, IVUPMJ, IVTDAY, IVRATO, IVAPSP, IVIDEM, IVAPSS, IVCIRV, IVADIND, IVBPIND, IVCARDNO FROM SIDTA.F4104 WHERE IVUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F4104"
    }
    # Item Pricing
    F4106 = @{
        Sql     = "SELECT BPITM, BPLITM, BPAITM, BPMCU, BPLOCN, BPLOTN, BPAN8, BPIGID, BPCGID, BPLOTG, BPFRMP, BPCRCD, BPUOM, BPEFTJ, BPEXDJ, BPUPRC, BPACRD, BPBSCD, BPLEDG, BPFVTR, BPFRMN, BPURCD, BPURDT, BPURAT, BPURAB, BPURRF, BPAPRS, BPUSER, BPPID, BPJOBN, BPUPMJ, BPTDAY FROM SIDTA.F4106 WHERE BPUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F4106"
    }
    # UOM Conversions
    F41002 = @{
        Sql     = "SELECT UMMCU, UMITM, UMUM, UMRUM, UMUSTR, UMCONV, UMCNV1, UMUSER, UMPID, UMJOBN, UMUPMJ, UMTDAY, UMEXPO, UMEXSO, UMPUPC, UMSEPC FROM SIDTA.F41002 WHERE UMUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F41002"
    }
    # Address Book
    F0101 = @{
        Sql     = "SELECT ABAN8, ABALKY, ABTAX, ABALPH, ABDC, ABMCU, ABSIC, ABLNGP, ABAT1, ABCM, ABTAXC, ABAT2, ABAT3, ABAT4, ABAT5, ABATP, ABATR, ABATPR, ABAB3, ABATE, ABSBLI, ABEFTB, ABAN81, ABAN82, ABAN83, ABAN84, ABAN86, ABAN85, ABAC01, ABAC02, ABAC03, ABAC04, ABAC05, ABAC06, ABAC07, ABAC08, ABAC09, ABAC10, ABAC11, ABAC12, ABAC13, ABAC14, ABAC15, ABAC16, ABAC17, ABAC18, ABAC19, ABAC20, ABAC21, ABAC22, ABAC23, ABAC24, ABAC25, ABAC26, ABAC27, ABAC28, ABAC29, ABAC30, ABGLBA, ABPTI, ABPDI, ABMSGA, ABRMK, ABTXCT, ABTX2, ABALP1, ABURCD, ABURDT, ABURAT, ABURAB, ABURRF, ABUSER, ABPID, ABUPMJ, ABJOBN, ABUPMT, ABPRGF, ABSCCLTP, ABTICKER, ABEXCHG, ABDUNS, ABCLASS01, ABCLASS02, ABCLASS03, ABCLASS04, ABCLASS05, ABNOE, ABGROWTHR, ABYEARSTAR, ABAEMPGP, ABACTIN, ABREVRNG, ABSYNCS, ABPERRS, ABCAAD FROM SIDTA.F0101 WHERE ABUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F0101"
    }
    # Customer Master
    F03012 = @{
        Sql     = "SELECT AIAN8, AICO, AIARC, AIMCUR, AIOBAR, AIAIDR, AIKCOR, AIDCAR, AIDTAR, AICRCD, AITXA1, AIEXR1, AIACL, AIHDAR, AITRAR, AISTTO, AIRYIN, AISTMT, AIARPY, AIATCS, AISITO, AISQNL, AIALGM, AICYCN, AIBO, AITSTA, AICKHC, AIDLC, AIDNLT, AIPLCR, AIRVDJ, AIDSO, AICMGR, AICLMG, AIDLQT, AIDLQJ, AINBRR, AICOLL, AINBR1, AINBR2, AINBR3, AINBCL, AIAFC, AIFD, AIFP, AICFCE, AIAB2, AIDT1J, AIDFIJ, AIDLIJ, AIABC1, AIABC2, AIABC3, AIFNDJ, AIDLP, AIDB, AIDNBJ, AITRW, AITWDJ, AIAVD, AICRCA, AIAD, AIAFCP, AIAFCY, AIASTY, AISPYE, AIAHB, AIALP, AIABAM, AIABA1, AIAPRC, AIMAXO, AIMINO, AIOYTD, AIOPY, AIPOPN, AIDAOJ, AIAN8R, AIBADT, AICPGP, AIORTP, AITRDC, AIINMG, AIEXHD, AIHOLD, AIROUT, AISTOP, AIZON, AICARS, AIDEL1, AIDEL2, AILTDT, AIFRTH, AIAFT, AIAPTS, AISBAL, AIBACK, AIPORQ, AIPRIO, AIARTO, AIINVC, AIICON, AIBLFR, AINIVD, AILEDJ, AIPLST, AIMORD, AICMC1, AICMR1, AICMC2, AICMR2, AIPALC, AIVUMD, AIWUMD, AIEDPM, AIEDII, AIEDCI, AIEDQD, AIEDAD, AIEDF1, AIEDF2, AISI01, AISI02, AISI03, AISI04, AISI05, AIURCD, AIURAT, AIURAB, AIURDT, AIURRF, AICP01, AIASN, AIDSPA, AICRMD, AIPLY, AIMAN8, AIARL, AIAMCR, AIAC01, AIAC02, AIAC03, AIAC04, AIAC05, AIAC06, AIAC07, AIAC08, AIAC09, AIAC10, AIAC11, AIAC12, AIAC13, AIAC14, AIAC15, AIAC16, AIAC17, AIAC18, AIAC19, AIAC20, AIAC21, AIAC22, AIAC23, AIAC24, AIAC25, AIAC26, AIAC27, AIAC28, AIAC29, AIAC30, AISLPG, AISLDW, AICFPP, AICFSP, AICFDF, AIRQ01, AIRQ02, AIDR03, AIDR04, AIRQ03, AIRQ04, AIRQ05, AIRQ06, AIRQ07, AIRQ08, AIDR08, AIDR09, AIRQ09, AIUSER, AIPID, AIUPMJ, AIUPMT, AIJOBN, AIPRGF, AIBYAL, AIBSC, AIASHL, AIPRSN, AIOPBO, AIAPSB, AITIER1, AIPWPCP, AICUSTS, AISTOF, AITERRID, AICIG, AITORG, AIDTEE, AISYNCS, AICAAD FROM SIDTA.F03012 WHERE AIUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F03012"
    }
    # Who's Who
    F0111 = @{
        Sql     = "SELECT WWAN8, WWIDLN, WWDSS5, WWMLNM, WWATTL, WWREM1, WWSLNM, WWALPH, WWDC, WWGNNM, WWMDNM, WWSRNM, WWTYC, WWW001, WWW002, WWW003, WWW004, WWW005, WWW006, WWW007, WWW008, WWW009, WWW010, WWMLN1, WWALP1, WWUSER, WWPID, WWUPMJ, WWJOBN, WWUPMT, WWNTYP, WWNICK, WWGEND, WWDDATE, WWDMON, WWDYR, WWWN001, WWWN002, WWWN003, WWWN004, WWWN005, WWWN006, WWWN007, WWWN008, WWWN009, WWWN010, WWFUCO, WWPCM, WWPCF, WWACTIN, WWCFRGUID, WWSYNCS, WWCAAD FROM SIDTA.F0111 WHERE WWUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F0111"
    }
    # Electronic Addresses
    F01151 = @{
        Sql     = "SELECT EAAN8, EAIDLN, EARCK7, EAETP, EAEMAL, EAUSER, EAPID, EAUPMJ, EAJOBN, EAUPMT, EAEHIER, EAEFOR, EAECLASS, EACFNO1, EAGEN1, EAFALGE, EASYNCS, EACAAD FROM SIDTA.F01151 WHERE EAUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F01151"
    }
    # Address Master
    F0116 = @{
        Sql     = "SELECT ALAN8, ALEFTB, ALEFTF, ALADD1, ALADD2, ALADD3, ALADD4, ALADDZ, ALCTY1, ALCOUN, ALADDS, ALCRTE, ALBKML, ALCTR, ALUSER, ALPID, ALUPMJ, ALJOBN, ALUPMT, ALSYNCS, ALCAAD FROM SIDTA.F0116 WHERE ALUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F0116"
    }
    # Phone Contacts
    F0115 = @{
        Sql     = "SELECT WPAN8, WPIDLN, WPRCK7, WPCNLN, WPPHTP, WPAR1, WPPH1, WPUSER, WPPID, WPUPMJ, WPJOBN, WPUPMT, WPCFNO1, WPGEN1, WPFALGE, WPSYNCS, WPCAAD FROM SIDTA.F0115 WHERE WPUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F0115"
    }
    # Sales Order Headers
    F4201 = @{
        Sql     = "SELECT SHKCOO, SHDOCO, SHDCTO, SHSFXO, SHMCU, SHCO, SHOKCO, SHOORN, SHOCTO, SHRKCO, SHRORN, SHRCTO, SHAN8, SHSHAN, SHPA8, SHDRQJ, SHTRDJ, SHPDDJ, SHOPDJ, SHADDJ, SHCNDJ, SHPEFJ, SHPPDJ, SHVR01, SHVR02, SHDEL1, SHDEL2, SHINMG, SHPTC, SHRYIN, SHASN, SHPRGP, SHTRDC, SHPCRT, SHTXA1, SHEXR1, SHTXCT, SHATXT, SHPRIO, SHBACK, SHSBAL, SHHOLD, SHPLST, SHINVC, SHNTR, SHANBY, SHCARS, SHMOT, SHCOT, SHROUT, SHSTOP, SHZON, SHCNID, SHFRTH, SHAFT, SHFUF1, SHFRTC, SHMORD, SHRCD, SHFUF2, SHOTOT, SHTOTC, SHWUMD, SHVUMD, SHAUTN, SHCACT, SHCEXP, SHSBLI, SHCRMD, SHCRRM, SHCRCD, SHCRR, SHLNGP, SHFAP, SHFCST, SHORBY, SHTKBY, SHURCD, SHURDT, SHURAT, SHURAB, SHURRF, SHUSER, SHPID, SHJOBN, SHUPMJ, SHTDAY, SHIR01, SHIR02, SHIR03, SHIR04, SHIR05, SHVR03, SHSOOR, SHPMDT, SHRSDT, SHRQSJ, SHPSTM, SHPDTT, SHOPTT, SHDRQT, SHADTM, SHADLJ, SHPBAN, SHITAN, SHFTAN, SHDVAN, SHDOC1, SHDCT4, SHCORD, SHBSC, SHBCRC, SHAUFT, SHAUFI, SHOPBO, SHOPTC, SHOPLD, SHOPBK, SHOPSB, SHOPPS, SHOPPL, SHOPMS, SHOPSS, SHOPBA, SHOPLL, SHPRAN8, SHOPPID, SHSDATTN, SHSPATTN, SHOTIND, SHPRCIDLN, SHCCIDLN, SHSHCCIDLN FROM SIDTA.F4201 WHERE SHUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F4201"
    }
    # Business Unit Master
    F0006 = @{
        Sql      = "SELECT MCMCU, MCSTYL, MCDC, MCLDM, MCCO, MCAN8, MCAN8O, MCCNTY, MCADDS, MCFMOD, MCDL01, MCDL02, MCDL03, MCDL04, MCRP01, MCRP02, MCRP03, MCRP04, MCRP05, MCRP06, MCRP07, MCRP08, MCRP09, MCRP10, MCRP11, MCRP12, MCRP13, MCRP14, MCRP15, MCRP16, MCRP17, MCRP18, MCRP19, MCRP20, MCRP21, MCRP22, MCRP23, MCRP24, MCRP25, MCRP26, MCRP27, MCRP28, MCRP29, MCRP30, MCTA, MCTXJS, MCTXA1, MCEXR1, MCTC01, MCTC02, MCTC03, MCTC04, MCTC05, MCTC06, MCTC07, MCTC08, MCTC09, MCTC10, MCND01, MCND02, MCND03, MCND04, MCND05, MCND06, MCND07, MCND08, MCND09, MCND10, MCCC01, MCCC02, MCCC03, MCCC04, MCCC05, MCCC06, MCCC07, MCCC08, MCCC09, MCCC10, MCPECC, MCALS, MCISS, MCGLBA, MCALCL, MCLMTH, MCLF, MCOBJ1, MCOBJ2, MCOBJ3, MCSUB1, MCTOU, MCSBLI, MCANPA, MCCT, MCCERT, MCMCUS, MCBTYP, MCPC, MCPCA, MCPCC, MCINTA, MCINTL, MCD1J, MCD2J, MCD3J, MCD4J, MCD5J, MCD6J, MCFPDJ, MCCAC, MCPAC, MCEEO, MCERC, MCUSER, MCPID, MCUPMJ, MCJOBN, MCUPMT, MCBPTP, MCAPSB, MCTSBU, MCRP31, MCRP32, MCRP33, MCRP34, MCRP35, MCRP36, MCRP37, MCRP38, MCRP39, MCRP40, MCRP41, MCRP42, MCRP43, MCRP44, MCRP45, MCRP46, MCRP47, MCRP48, MCRP49, MCRP50, MCAN8GCA1, MCAN8GCA2, MCAN8GCA3, MCAN8GCA4, MCAN8GCA5, MCRMCU1, MCDOCO, MCPCTN, MCCLNU, MCBUCA, MCADJENT, MCUAFL FROM SIDTA.F0006 WHERE MCUPMJ = $jdeJulianToday"
        CsvBase  = "SIDTA_F0006"
    }
    # Sales Order Details
    F4211 = @{
        Sql      = "SELECT SDKCOO, SDDOCO, SDDCTO, SDLNID, SDSFXO, SDMCU, SDCO, SDOKCO, SDOORN, SDOCTO, SDOGNO, SDRKCO, SDRORN, SDRCTO, SDRLLN, SDDMCT, SDDMCS, SDAN8, SDSHAN, SDPA8, SDDRQJ, SDTRDJ, SDPDDJ, SDADDJ, SDIVD, SDCNDJ, SDDGL, SDRSDJ, SDPEFJ, SDPPDJ, SDVR01, SDVR02, SDITM, SDLITM, SDAITM, SDLOCN, SDLOTN, SDFRGD, SDTHGD, SDFRMP, SDTHRP, SDEXDP, SDDSC1, SDDSC2, SDLNTY, SDNXTR, SDLTTR, SDEMCU, SDRLIT, SDKTLN, SDCPNT, SDRKIT, SDKTP, SDSRP1, SDSRP2, SDSRP3, SDSRP4, SDSRP5, SDPRP1, SDPRP2, SDPRP3, SDPRP4, SDPRP5, SDUOM, SDUORG, SDSOQS, SDSOBK, SDSOCN, SDSONE, SDUOPN, SDQTYT, SDQRLV, SDCOMM, SDOTQY, SDUPRC, SDAEXP, SDAOPN, SDPROV, SDTPC, SDAPUM, SDLPRC, SDUNCS, SDECST, SDCSTO, SDTCST, SDINMG, SDPTC, SDRYIN, SDDTBS, SDTRDC, SDFUN2, SDASN, SDPRGR, SDCLVL, SDCADC, SDKCO, SDDOC, SDDCT, SDODOC, SDODCT, SDOKC, SDPSN, SDDELN, SDTAX1, SDTXA1, SDEXR1, SDATXT, SDPRIO, SDRESL, SDBACK, SDSBAL, SDAPTS, SDLOB, SDEUSE, SDDTYS, SDNTR, SDVEND, SDCARS, SDMOT, SDROUT, SDSTOP, SDZON, SDCNID, SDFRTH, SDSHCM, SDSHCN, SDSERN, SDUOM1, SDPQOR, SDUOM2, SDSQOR, SDUOM4, SDITWT, SDWTUM, SDITVL, SDVLUM, SDRPRC, SDORPR, SDORP, SDCMGP, SDGLC, SDCTRY, SDFY, SDSO01, SDSO02, SDSO03, SDSO04, SDSO05, SDSO06, SDSO07, SDSO08, SDSO09, SDSO10, SDSO11, SDSO12, SDSO13, SDSO14, SDSO15, SDACOM, SDCMCG, SDRCD, SDGRWT, SDGWUM, SDSBL, SDSBLT, SDLCOD, SDUPC1, SDUPC2, SDUPC3, SDSWMS, SDUNCD, SDCRMD, SDCRCD, SDCRR, SDFPRC, SDFUP, SDFEA, SDFUC, SDFEC, SDURCD, SDURDT, SDURAT, SDURAB, SDURRF, SDTORG, SDUSER, SDPID, SDJOBN, SDUPMJ, SDTDAY, SDSO16, SDSO17, SDSO18, SDSO19, SDSO20, SDIR01, SDIR02, SDIR03, SDIR04, SDIR05, SDSOOR, SDVR03, SDDEID, SDPSIG, SDRLNU, SDPMDT, SDRLTM, SDRLDJ, SDDRQT, SDADTM, SDOPTT, SDPDTT, SDPSTM, SDXDCK, SDXPTY, SDDUAL, SDBSC, SDCBSC, SDCORD, SDDVAN, SDPEND, SDRFRV, SDMCLN, SDSHPN, SDRSDT, SDPRJM, SDOSEQ, SDMERL, SDHOLD, SDHDBU, SDDMBU, SDBCRC, SDODLN, SDOPDJ, SDXKCO, SDXORN, SDXCTO, SDXLLN, SDXSFX, SDPOE, SDPMTO, SDANBY, SDPMTN, SDNUMB, SDAAID, SDSPATTN, SDPRAN8, SDPRCIDLN, SDCCIDLN, SDSHCCIDLN, SDOPPID, SDOSTP, SDUKID, SDCATNM, SDALLOC, SDFULPID, SDALLSTS, SDOSCORE, SDOSCOREO, SDCMCO, SDKITID, SDKITAMTDOM, SDKITAMTFOR, SDKITDIRTY, SDOCITT, SDOCCARDNO FROM SIDTA.F4211 WHERE SDLTTR != '398' AND SDUPMJ = $jdeJulianToday"
        CsvBase  = "SIDTA_F4211"
    }
    # SO Detail History
    F42119 = @{
        Sql      = "SELECT SDKCOO, SDDOCO, SDDCTO, SDLNID, SDSFXO, SDMCU, SDCO, SDOKCO, SDOORN, SDOCTO, SDOGNO, SDRKCO, SDRORN, SDRCTO, SDRLLN, SDDMCT, SDDMCS, SDAN8, SDSHAN, SDPA8, SDDRQJ, SDTRDJ, SDPDDJ, SDADDJ, SDIVD, SDCNDJ, SDDGL, SDRSDJ, SDPEFJ, SDPPDJ, SDVR01, SDVR02, SDITM, SDLITM, SDAITM, SDLOCN, SDLOTN, SDFRGD, SDTHGD, SDFRMP, SDTHRP, SDEXDP, SDDSC1, SDDSC2, SDLNTY, SDNXTR, SDLTTR, SDEMCU, SDRLIT, SDKTLN, SDCPNT, SDRKIT, SDKTP, SDSRP1, SDSRP2, SDSRP3, SDSRP4, SDSRP5, SDPRP1, SDPRP2, SDPRP3, SDPRP4, SDPRP5, SDUOM, SDUORG, SDSOQS, SDSOBK, SDSOCN, SDSONE, SDUOPN, SDQTYT, SDQRLV, SDCOMM, SDOTQY, SDUPRC, SDAEXP, SDAOPN, SDPROV, SDTPC, SDAPUM, SDLPRC, SDUNCS, SDECST, SDCSTO, SDTCST, SDINMG, SDPTC, SDRYIN, SDDTBS, SDTRDC, SDFUN2, SDASN, SDPRGR, SDCLVL, SDCADC, SDKCO, SDDOC, SDDCT, SDODOC, SDODCT, SDOKC, SDPSN, SDDELN, SDTAX1, SDTXA1, SDEXR1, SDATXT, SDPRIO, SDRESL, SDBACK, SDSBAL, SDAPTS, SDLOB, SDEUSE, SDDTYS, SDNTR, SDVEND, SDCARS, SDMOT, SDROUT, SDSTOP, SDZON, SDCNID, SDFRTH, SDSHCM, SDSHCN, SDSERN, SDUOM1, SDPQOR, SDUOM2, SDSQOR, SDUOM4, SDITWT, SDWTUM, SDITVL, SDVLUM, SDRPRC, SDORPR, SDORP, SDCMGP, SDGLC, SDCTRY, SDFY, SDSO01, SDSO02, SDSO03, SDSO04, SDSO05, SDSO06, SDSO07, SDSO08, SDSO09, SDSO10, SDSO11, SDSO12, SDSO13, SDSO14, SDSO15, SDACOM, SDCMCG, SDRCD, SDGRWT, SDGWUM, SDSBL, SDSBLT, SDLCOD, SDUPC1, SDUPC2, SDUPC3, SDSWMS, SDUNCD, SDCRMD, SDCRCD, SDCRR, SDFPRC, SDFUP, SDFEA, SDFUC, SDFEC, SDURCD, SDURDT, SDURAT, SDURAB, SDURRF, SDTORG, SDUSER, SDPID, SDJOBN, SDUPMJ, SDTDAY, SDSO16, SDSO17, SDSO18, SDSO19, SDSO20, SDIR01, SDIR02, SDIR03, SDIR04, SDIR05, SDSOOR, SDVR03, SDDEID, SDPSIG, SDRLNU, SDPMDT, SDRLTM, SDRLDJ, SDDRQT, SDADTM, SDOPTT, SDPDTT, SDPSTM, SDXDCK, SDXPTY, SDDUAL, SDBSC, SDCBSC, SDCORD, SDDVAN, SDPEND, SDRFRV, SDMCLN, SDSHPN, SDRSDT, SDPRJM, SDOSEQ, SDMERL, SDHOLD, SDHDBU, SDDMBU, SDBCRC, SDODLN, SDOPDJ, SDXKCO, SDXORN, SDXCTO, SDXLLN, SDXSFX, SDPOE, SDPMTO, SDANBY, SDPMTN, SDNUMB, SDAAID, SDSPATTN, SDPRAN8, SDPRCIDLN, SDCCIDLN, SDSHCCIDLN, SDOPPID, SDOSTP, SDUKID, SDCATNM, SDALLOC, SDFULPID, SDALLSTS, SDOSCORE, SDOSCOREO, SDCMCO, SDKITID, SDKITAMTDOM, SDKITAMTFOR, SDKITDIRTY, SDOCITT, SDOCCARDNO FROM SIDTA.F42119 WHERE SDUPMJ > 125031 AND SDDGL = $jdeJulianToday"
        CsvBase  = "SIDTA_F42119"
    }
    F4008 = @{
        Sql     = "SELECT TATXA1, TATAXA, TATA1, TATXR1, TATA2, TATXR2, TATA3, TATXR3, TATA4, TATXR4, TATA5, TATXR5, TAEFDJ, TAEFTJ, TAGL01, TAGL02, TAGL03, TAGL04, TAGL05, TAITM, TALITM, TAAITM, TAUOM, TAFVTY, TAMTAX, TATC1, TATC2, TATC3, TATC4, TATC5, TATT1, TATT2, TATT3, TATT4, TATT5 FROM SIDTA.F4008"
        CsvBase = "SIDTA_F4008"
    }
    # AR Ledger
    F03B11 = @{
        Sql     = "SELECT RPDOC, RPDCT, RPKCO, RPSFX, RPAN8, RPDGJ, RPDIVJ, RPICUT, RPICU, RPDICJ, RPFY, RPCTRY, RPPN, RPCO, RPGLC, RPAID, RPPA8, RPAN8J, RPPYR, RPPOST, RPISTR, RPBALJ, RPPST, RPAG, RPAAP, RPADSC, RPADSA, RPATXA, RPATXN, RPSTAM, RPBCRC, RPCRRM, RPCRCD, RPCRR, RPDMCD, RPACR, RPFAP, RPCDS, RPCDSA, RPCTXA, RPCTXN, RPCTAM, RPTXA1, RPEXR1, RPDSVJ, RPGLBA, RPAM, RPAID2, RPAM2, RPMCU, RPOBJ, RPSUB, RPSBLT, RPSBL, RPPTC, RPDDJ, RPDDNJ, RPRDDJ, RPRDSJ, RPLFCJ, RPSMTJ, RPNBRR, RPRDRL, RPRMDS, RPCOLL, RPCORC, RPAFC, RPDNLT, RPRSCO, RPODOC, RPODCT, RPOKCO, RPOSFX, RPVINV, RPPO, RPPDCT, RPPKCO, RPDCTO, RPLNID, RPSDOC, RPSDCT, RPSKCO, RPSFXO, RPVLDT, RPCMC1, RPVR01, RPUNIT, RPMCU2, RPRMK, RPALPH, RPRF, RPDRF, RPCTL, RPFNLP, RPITM, RPU, RPUM, RPALT6, RPRYIN, RPVDGJ, RPVOD, RPRP1, RPRP2, RPRP3, RPAR01, RPAR02, RPAR03, RPAR04, RPAR05, RPAR06, RPAR07, RPAR08, RPAR09, RPAR10, RPISTC, RPPYID, RPURC1, RPURDT, RPURAT, RPURAB, RPURRF, RPRNID, RPPPDI, RPTORG, RPUSER, RPJCL, RPPID, RPUPMJ, RPUPMT, RPDDEX, RPJOBN, RPHCRR, RPHDGJ, RPSHPN, RPDTXS, RPOMOD, RPCLMG, RPCMGR, RPATAD, RPCTAD, RPNRTA, RPFNRT, RPPRGF, RPGFL1, RPGFL2, RPDOCO, RPKCOO, RPSOTF, RPDTPB, RPERDJ, RPPWPG, RPNETTCID, RPNETDOC, RPNETRC5, RPNETST, RPAJCL, RPRMR1 FROM SIDTA.F03B11 WHERE RPUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F03B11"
    }
    # Inventory Balances
    F41021 = @{
        Sql     = "SELECT LIITM, LIMCU, LILOCN, LILOTN, LIPBIN, LIGLPT, LILOTS, LILRCJ, LIPQOH, LIPBCK, LIPREQ, LIQWBO, LIOT1P, LIOT2P, LIOT1A, LIHCOM, LIPCOM, LIFCOM, LIFUN1, LIQOWO, LIQTTR, LIQTIN, LIQONL, LIQTRI, LIQTRO, LINCDJ, LIQTY1, LIQTY2, LIURAB, LIURRF, LIURAT, LIURCD, LIJOBN, LIPID, LIUPMJ, LIUSER, LITDAY, LIURDT, LIQTO1, LIQTO2, LIHCMS, LIPJCM, LIPJDM, LISCMS, LISIBW, LISOBW, LISQOH, LISQWO, LISREQ, LISWHC, LISWSC, LICHDF, LIWPDF, LICFGSID FROM SIDTA.F41021 WHERE LIUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F41021"
    }
    # Parent Account Ref
    F0150 = @{
        Sql     = "SELECT MAOSTP, MAPA8, MAAN8, MADSS7, MABEFD, MAEEFD, MARMK, MAUSER, MAUPMJ, MAPID, MAJOBN, MAUPMT, MASYNCS FROM SIDTA.F0150 WHERE MAUPMJ = $jdeJulianToday"
        CsvBase = "SIDTA_F0150"
    }
    # User Defined Codes
    F0005 = @{
        Sql     = "SELECT DRSY, DRRT, DRKY, DRDL01, DRDL02, DRSPHD, DRUDCO, DRHRDC, DRUSER, DRPID, DRUPMJ, DRJOBN, DRUPMT FROM SICTL.F0005 WHERE DRUPMJ = $jdeJulianToday"
        CsvBase = "SICTL_F0005"
    }
    F40942 = @{
        Sql     = "SELECT CKCPGP, CKCGP1, CKCGP2, CKCGP3, CKCGP4, CKCGP5, CKCGP6, CKCGP7, CKCGP8, CKCGP9, CKCGP10, CKCGID FROM SIDTA.F40942"
        CsvBase = "SICTL_F40942"
    }    
    # Add more queries here
}

# === Build Connection String ===
$connString = "User Id=$user;Password=$password;Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$oci_host)(PORT=$port)(CONNECT_TIMEOUT=30))(CONNECT_DATA=(SERVICE_NAME=$serviceName)))"

# === Function: Export Query to CSV with Error Handling ===
function Start-OracleExportJob {
    param (
        [string]$Sql,
        [string]$CsvBase,
        [string]$Schema,
        [string]$User,
        [string]$Password,
        [string]$ConnStr,
        [string]$Timestamp,
        [string]$DllPath,
        [string]$DataFilesPath,
        [string]$OciHost,
        [string]$Port
    )

    Start-Job -ScriptBlock {
        param($Sql, $CsvBase, $Schema, $User, $Password, $ConnStr, $Timestamp, $DllPath, $DataFilesPath, $OciHost, $Port)

        $conn = $null
        $rdr = $null
        $cmd = $null
        $finalSql = $null

        try {
            # #region agent log
            $logDir = Join-Path (Split-Path $DataFilesPath -Parent) ".cursor"
            if (-not (Test-Path $logDir)) {
                New-Item -ItemType Directory -Path $logDir -Force | Out-Null
            }
            $logPath = Join-Path $logDir "debug.log"
            $logEntry = @{
                sessionId = "debug-session"
                runId = "run1"
                hypothesisId = "A"
                location = "Extract_JDE92_Data.ps1:164"
                message = "Before Add-Type attempt"
                data = @{
                    CsvBase = $CsvBase
                    DllPath = $DllPath
                }
                timestamp = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
            }
            $logEntry | ConvertTo-Json -Compress | Add-Content -Path $logPath -Encoding UTF8
            # #endregion agent log
            
            try {
                Add-Type -Path $DllPath
                # #region agent log
                $logEntry2 = @{
                    sessionId = "debug-session"
                    runId = "run1"
                    hypothesisId = "A"
                    location = "Extract_JDE92_Data.ps1:164"
                    message = "Add-Type succeeded"
                    data = @{ CsvBase = $CsvBase }
                    timestamp = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
                }
                $logEntry2 | ConvertTo-Json -Compress | Add-Content -Path $logPath -Encoding UTF8
                # #endregion agent log
            } catch {
                # #region agent log
                $logDir = Join-Path (Split-Path $DataFilesPath -Parent) ".cursor"
                if (-not (Test-Path $logDir)) {
                    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
                }
                $logPath = Join-Path $logDir "debug.log"
                $logEntry3 = @{
                    sessionId = "debug-session"
                    runId = "run1"
                    hypothesisId = "A"
                    location = "Extract_JDE92_Data.ps1:195"
                    message = "Add-Type exception caught"
                    data = @{ 
                        CsvBase = $CsvBase
                        ErrorMessage = $_.Exception.Message
                        ErrorType = $_.Exception.GetType().FullName
                        CanUseOracleType = $false
                    }
                    timestamp = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
                }
                # #endregion agent log
                
                $canUseOracleType = $false
                try {
                    $null = [Oracle.ManagedDataAccess.Client.OracleConnection]
                    $canUseOracleType = $true
                } catch {
                    $canUseOracleType = $false
                }
                
                $logEntry3.data.CanUseOracleType = $canUseOracleType
                $isAssemblyError = ($_.Exception.Message -match "Assembly" -and $_.Exception.Message -match "already loaded") -or
                                   ($_.Exception.Message -match "same name") -or
                                   ($canUseOracleType -eq $true)
                
                $logEntry3.data.MatchesAssemblyError = $isAssemblyError
                $logEntry3 | ConvertTo-Json -Compress | Add-Content -Path $logPath -Encoding UTF8
                # #endregion agent log
                
                if ($isAssemblyError -or $canUseOracleType) {
                    # #region agent log
                    $logEntry4 = @{
                        sessionId = "debug-session"
                        runId = "run1"
                        hypothesisId = "A"
                        location = "Extract_JDE92_Data.ps1:225"
                        message = "Assembly already loaded or available, continuing"
                        data = @{ CsvBase = $CsvBase; CanUseOracleType = $canUseOracleType }
                        timestamp = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
                    }
                    $logEntry4 | ConvertTo-Json -Compress | Add-Content -Path $logPath -Encoding UTF8
                    # #endregion agent log
                } else {
                    throw
                }
            }
            
            # #region agent log
            $logEntry4 = @{
                sessionId = "debug-session"
                runId = "run1"
                hypothesisId = "B"
                location = "Extract_JDE92_Data.ps1:166"
                message = "Before creating OracleConnection"
                data = @{ CsvBase = $CsvBase; OracleTypeAvailable = $false }
                timestamp = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
            }
            try {
                $null = [Oracle.ManagedDataAccess.Client.OracleConnection]
                $logEntry4.data.OracleTypeAvailable = $true
            } catch {
                $logEntry4.data.OracleTypeError = $_.Exception.Message
            }
            $logEntry4 | ConvertTo-Json -Compress | Add-Content -Path $logPath -Encoding UTF8
            # #endregion agent log

            $conn = New-Object Oracle.ManagedDataAccess.Client.OracleConnection($ConnStr)
            try {
                $conn.Open()
                Write-Host "[$CsvBase] Connected to Oracle as $User"
            } catch {
                $errorMsg = "Failed to connect to Oracle database. "
                if ($_.Exception.Message -match "timeout" -or $_.Exception.Message -match "Connection request timed out") {
                    $errorMsg += "Connection timeout - check network connectivity, firewall rules, and Oracle server status at $OciHost`:$Port"
                } else {
                    $errorMsg += $_.Exception.Message
                }
                throw $errorMsg
            }

            $finalSql = $Sql
            if ($Schema -and $Schema -ne $User -and -not $Sql -match "FROM\s+(SICTL|SIDTA)\.") {
                $finalSql = $Sql -replace "(?i)(FROM\s+)([A-Z0-9_]+)(\s|WHERE|JOIN|$)", "`$1$Schema.`$2`$3"
                $finalSql = $finalSql -replace "(?i)(JOIN\s+)([A-Z0-9_]+)(\s|ON|$)", "`$1$Schema.`$2`$3"
                Write-Host "[$CsvBase] Added schema prefix $Schema to query"
            }

            Write-Host "[$CsvBase] Executing query..."
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = $finalSql
            $cmd.CommandTimeout = 600
            $rdr = $cmd.ExecuteReader()

            $table = New-Object System.Data.DataTable
            $table.Load($rdr)
            $rowCount = $table.Rows.Count

            $fileName = "${CsvBase}_${Timestamp}.csv"
            $csvPath = Join-Path $DataFilesPath $fileName
            $table | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
            Write-Host "[$CsvBase] Exported $rowCount rows to $csvPath" -ForegroundColor Green
            return @{ Success = $true; CsvBase = $CsvBase; RowCount = $rowCount; Path = $csvPath }
        }
        catch {
            $errorMsg = "[$CsvBase] ERROR: $($_.Exception.Message)"
            if ($finalSql) {
                $errorMsg += "`nSQL: $finalSql"
            }
            Write-Host $errorMsg -ForegroundColor Red
            return @{ Success = $false; CsvBase = $CsvBase; Error = $errorMsg }
        }
        finally {
            if ($rdr) { $rdr.Dispose() }
            if ($cmd) { $cmd.Dispose() }
            if ($conn) { $conn.Close(); $conn.Dispose() }
        }
    } -ArgumentList $Sql, $CsvBase, $Schema, $User, $Password, $ConnStr, $Timestamp, $DllPath, $DataFilesPath, $OciHost, $Port
}

# === Clean up any existing jobs ===
$existingJobs = Get-Job -ErrorAction SilentlyContinue
if ($existingJobs) {
    Write-Host "Cleaning up $($existingJobs.Count) existing job(s)..." -ForegroundColor Yellow
    $existingJobs | Remove-Job -Force -ErrorAction SilentlyContinue
}

# === Ensure Data Files Directory Exists ===
if (-not (Test-Path $dataFilesPath)) {
    New-Item -ItemType Directory -Path $dataFilesPath -Force | Out-Null
    Write-Host "Created directory: $dataFilesPath" -ForegroundColor Yellow
}

# === Start Jobs with Throttling (Max 4 Parallel) ===
Write-Host "`nFound $($queries.Count) query/queries to process" -ForegroundColor Cyan
Write-Host "Starting $($queries.Count) export jobs (max 4 parallel)..." -ForegroundColor Cyan
$maxParallelJobs = 4
$jobs = @()
$jobQueue = @()

foreach ($key in $queries.Keys) {
    $q = $queries[$key]
    Write-Host "  Adding query to queue: $($q.CsvBase)" -ForegroundColor Gray
    $jobQueue += @{
        Sql = $q.Sql
        CsvBase = $q.CsvBase
    }
}
Write-Host "Job queue contains $($jobQueue.Count) item(s)" -ForegroundColor Cyan

$completedJobs = @()
$runningJobs = @()

while ($jobQueue.Count -gt 0 -or $runningJobs.Count -gt 0) {
    while ($runningJobs.Count -lt $maxParallelJobs -and $jobQueue.Count -gt 0) {
        $jobItem = $jobQueue[0]
        if ($jobQueue.Count -eq 1) {
            $jobQueue = @()
        } else {
            $jobQueue = $jobQueue[1..($jobQueue.Count - 1)]
        }
        
        $job = Start-OracleExportJob -Sql $jobItem.Sql -CsvBase $jobItem.CsvBase -Schema $targetSchema -User $user -Password $password -ConnStr $connString -Timestamp $timestamp -DllPath $dllPath -DataFilesPath $dataFilesPath -OciHost $oci_host -Port $port
        $runningJobs += $job
        Write-Host "[$($jobItem.CsvBase)] Started job (Running: $($runningJobs.Count)/$maxParallelJobs, Queued: $($jobQueue.Count))" -ForegroundColor Yellow
    }
    
    $completed = $runningJobs | Where-Object { $_.State -eq 'Completed' -or $_.State -eq 'Failed' }
    foreach ($job in $completed) {
        $completedJobs += $job
        $runningJobs = $runningJobs | Where-Object { $_.Id -ne $job.Id }
    }
    
    if ($runningJobs.Count -ge $maxParallelJobs -or ($runningJobs.Count -gt 0 -and $jobQueue.Count -gt 0)) {
        Start-Sleep -Milliseconds 500
    }
}

$jobs = $completedJobs
Write-Host "All jobs completed. Collecting results..." -ForegroundColor Cyan

# === Collect Job Output and Track Results ===
$results = @()
$successCount = 0
$failureCount = 0
$totalRows = 0

foreach ($job in $jobs) {
    $result = Receive-Job -Job $job
    $results += $result
    
    if ($result.Success) {
        $successCount++
        $totalRows += $result.RowCount
    } else {
        $failureCount++
        "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $($result.Error)" | Out-File -FilePath $logFile -Append -Encoding UTF8
    }
    
    Remove-Job -Job $job
}

# === Summary ===
Write-Host "`n=== Export Summary ===" -ForegroundColor Cyan
Write-Host "Successful: $successCount" -ForegroundColor Green
Write-Host "Failed: $failureCount" -ForegroundColor $(if ($failureCount -gt 0) { "Red" } else { "Green" })
Write-Host "Total Rows Exported: $totalRows" -ForegroundColor Green

if ($failureCount -gt 0) {
    Write-Host "`nErrors logged to: $logFile" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "`nAll exports complete successfully!" -ForegroundColor Green
}