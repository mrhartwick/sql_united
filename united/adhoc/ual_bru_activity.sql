-- simply the activity table portion of our standard activity-impression-click triumvirate. The route case statement exceeds the 8,000 character limit of the openQuery function, so a separate table is needed.

create table diap01.mec_us_united_20056.ual_bru_activity
(
    "date"   date not null,
    order_id int  not null,
    site_id  int  not null,
    page_id  int  not null,
    imps     int  not null,
    clicks   int  not null,
    con      int  not null,
    tix      int  not null,
    ct_rev      decimal  not null,
    vt_rev      decimal  not null
);

insert into diap01.mec_us_united_20056.ual_bru_activity
("date", order_id, site_id, page_id, imps, clicks, con, tix, ct_rev, vt_rev)

    (select
cast (c9.click_time as date ) as "date",
c9.order_id as order_id,
c9.site_id as site_id,
c9.page_id as page_id,
0 as imps,
0 as clicks,
sum(case when c9.event_id=1 or c9.event_id=2 then 1 else 0 end ) as con,
sum(case when c9.event_id=1 or c9.event_id=2 then c9.quantity else 0 end ) as tix,
sum(case when c9.event_id=1 then revenue/rates.EXCHANGE_RATE else 0 end ) as ct_rev,
sum(case when c9.event_id=2 then revenue/rates.EXCHANGE_RATE else 0 end ) as vt_rev

from (
select
*,
    case when ((regexp_like(c.o1,'(AB[EQ])','ib') or regexp_like(c.o2,'(AB[EQ])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(AC[KV])','ib') or regexp_like(c.o2,'(AC[KV])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(AT[LW])','ib') or regexp_like(c.o2,'(AT[LW])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(AV[LP])','ib') or regexp_like(c.o2,'(AV[LP])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(BF[IL])','ib') or regexp_like(c.o2,'(BF[IL])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(BI[LS])','ib') or regexp_like(c.o2,'(BI[LS])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(BO[IS])','ib') or regexp_like(c.o2,'(BO[IS])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(BT[RV])','ib') or regexp_like(c.o2,'(BT[RV])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(BU[FR])','ib') or regexp_like(c.o2,'(BU[FR])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CA[EK])','ib') or regexp_like(c.o2,'(CA[EK])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CH[AOS])','ib') or regexp_like(c.o2,'(CH[AOS])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CL[ELT])','ib') or regexp_like(c.o2,'(CL[ELT])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CM[HIX])','ib') or regexp_like(c.o2,'(CM[HIX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CO[DSU])','ib') or regexp_like(c.o2,'(CO[DSU])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CP[RSX])','ib') or regexp_like(c.o2,'(CP[RSX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(CR[PW])','ib') or regexp_like(c.o2,'(CR[PW])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(FA[IRTY])','ib') or regexp_like(c.o2,'(FA[IRTY])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(GR[BKR])','ib') or regexp_like(c.o2,'(GR[BKR])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(GS[OP])','ib') or regexp_like(c.o2,'(GS[OP])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(GU[CM])','ib') or regexp_like(c.o2,'(GU[CM])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(IA[DH])','ib') or regexp_like(c.o2,'(IA[DH])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(IT[HO])','ib') or regexp_like(c.o2,'(IT[HO])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(JA[CNX])','ib') or regexp_like(c.o2,'(JA[CNX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(LA[NRSX])','ib') or regexp_like(c.o2,'(LA[NRSX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(LG[AB])','ib') or regexp_like(c.o2,'(LG[AB])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(LI[HT])','ib') or regexp_like(c.o2,'(LI[HT])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MA[FZ])','ib') or regexp_like(c.o2,'(MA[FZ])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MC[IO])','ib') or regexp_like(c.o2,'(MC[IO])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MD[TW])','ib') or regexp_like(c.o2,'(MD[TW])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MF[ER])','ib') or regexp_like(c.o2,'(MF[ER])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MK[CEG])','ib') or regexp_like(c.o2,'(MK[CEG])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(ML[IU])','ib') or regexp_like(c.o2,'(ML[IU])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MO[BT])','ib') or regexp_like(c.o2,'(MO[BT])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(MS[NOPY])','ib') or regexp_like(c.o2,'(MS[NOPY])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(OR[DF])','ib') or regexp_like(c.o2,'(OR[DF])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(PH[LX])','ib') or regexp_like(c.o2,'(PH[LX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(PI[AET])','ib') or regexp_like(c.o2,'(PI[AET])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(PS[CP])','ib') or regexp_like(c.o2,'(PS[CP])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(RD[DMU])','ib') or regexp_like(c.o2,'(RD[DMU])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(RO[AC])','ib') or regexp_like(c.o2,'(RO[AC])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(RS[TW])','ib') or regexp_like(c.o2,'(RS[TW])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(SA[NTV])','ib') or regexp_like(c.o2,'(SA[NTV])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(SB[ANP])','ib') or regexp_like(c.o2,'(SB[ANP])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(SG[FU])','ib') or regexp_like(c.o2,'(SG[FU])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(SJ[CU])','ib') or regexp_like(c.o2,'(SJ[CU])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(ST[LSTX])','ib') or regexp_like(c.o2,'(ST[LSTX])','ib')) and (d1='BRU' or d2='BRU'))
or ((regexp_like(c.o1,'(TU[LS])','ib') or regexp_like(c.o2,'(TU[LS])','ib')) and (d1='BRU' or d2='BRU'))
or ((c.o1 = 'AEX' or c.o2 = 'AEX') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'AGS' or c.o2 = 'AGS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ALB' or c.o2 = 'ALB') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'AMA' or c.o2 = 'AMA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ANC' or c.o2 = 'ANC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ASE' or c.o2 = 'ASE') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'AUS' or c.o2 = 'AUS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'AZO' or c.o2 = 'AZO') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BDL' or c.o2 = 'BDL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BGR' or c.o2 = 'BGR') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BHM' or c.o2 = 'BHM') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BKL' or c.o2 = 'BKL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BNA' or c.o2 = 'BNA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BPT' or c.o2 = 'BPT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BQN' or c.o2 = 'BQN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BRO' or c.o2 = 'BRO') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BWI' or c.o2 = 'BWI') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'BZN' or c.o2 = 'BZN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'CID' or c.o2 = 'CID') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'CVG' or c.o2 = 'CVG') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'CWA' or c.o2 = 'CWA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DAY' or c.o2 = 'DAY') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DCA' or c.o2 = 'DCA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DEN' or c.o2 = 'DEN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DFW' or c.o2 = 'DFW') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DLH' or c.o2 = 'DLH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DRO' or c.o2 = 'DRO') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DSM' or c.o2 = 'DSM') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DTW' or c.o2 = 'DTW') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'DVL' or c.o2 = 'DVL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EAU' or c.o2 = 'EAU') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ECP' or c.o2 = 'ECP') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EGE' or c.o2 = 'EGE') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ELP' or c.o2 = 'ELP') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ERI' or c.o2 = 'ERI') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EUG' or c.o2 = 'EUG') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EVV' or c.o2 = 'EVV') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EWR' or c.o2 = 'EWR') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'EYW' or c.o2 = 'EYW') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'FCA' or c.o2 = 'FCA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'FLL' or c.o2 = 'FLL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'FNT' or c.o2 = 'FNT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'FSD' or c.o2 = 'FSD') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'FWA' or c.o2 = 'FWA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'GCC' or c.o2 = 'GCC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'GEG' or c.o2 = 'GEG') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'GJT' or c.o2 = 'GJT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'GPT' or c.o2 = 'GPT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'GTF' or c.o2 = 'GTF') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HDN' or c.o2 = 'HDN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HLN' or c.o2 = 'HLN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HNL' or c.o2 = 'HNL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HOB' or c.o2 = 'HOB') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HPN' or c.o2 = 'HPN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HRL' or c.o2 = 'HRL') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HSV' or c.o2 = 'HSV') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'HYS' or c.o2 = 'HYS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ICT' or c.o2 = 'ICT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'IDA' or c.o2 = 'IDA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'IND' or c.o2 = 'IND') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ISN' or c.o2 = 'ISN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'JMS' or c.o2 = 'JMS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'KOA' or c.o2 = 'KOA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LBB' or c.o2 = 'LBB') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LCH' or c.o2 = 'LCH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LEX' or c.o2 = 'LEX') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LFT' or c.o2 = 'LFT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LNK' or c.o2 = 'LNK') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'LRD' or c.o2 = 'LRD') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MBS' or c.o2 = 'MBS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MEM' or c.o2 = 'MEM') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MHT' or c.o2 = 'MHT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MIA' or c.o2 = 'MIA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MMH' or c.o2 = 'MMH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MRY' or c.o2 = 'MRY') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MTJ' or c.o2 = 'MTJ') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'MYR' or c.o2 = 'MYR') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'OAK' or c.o2 = 'OAK') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'OGG' or c.o2 = 'OGG') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'OKC' or c.o2 = 'OKC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'OMA' or c.o2 = 'OMA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'ONT' or c.o2 = 'ONT') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'OTH' or c.o2 = 'OTH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PAH' or c.o2 = 'PAH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PBI' or c.o2 = 'PBI') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PDX' or c.o2 = 'PDX') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PNS' or c.o2 = 'PNS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PTK' or c.o2 = 'PTK') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PVD' or c.o2 = 'PVD') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'PWM' or c.o2 = 'PWM') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'RAP' or c.o2 = 'RAP') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'RIC' or c.o2 = 'RIC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'RKS' or c.o2 = 'RKS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'RNO' or c.o2 = 'RNO') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SCE' or c.o2 = 'SCE') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SDF' or c.o2 = 'SDF') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SEA' or c.o2 = 'SEA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SFO' or c.o2 = 'SFO') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SHV' or c.o2 = 'SHV') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SLC' or c.o2 = 'SLC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SMF' or c.o2 = 'SMF') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SNA' or c.o2 = 'SNA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SPI' or c.o2 = 'SPI') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SRQ' or c.o2 = 'SRQ') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SUN' or c.o2 = 'SUN') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'SYR' or c.o2 = 'SYR') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'TLH' or c.o2 = 'TLH') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'TPA' or c.o2 = 'TPA') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'TVC' or c.o2 = 'TVC') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'TYS' or c.o2 = 'TYS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'VCV' or c.o2 = 'VCV') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'VPS' or c.o2 = 'VPS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'VQS' or c.o2 = 'VQS') and (c.d1 = 'BRU' or c.d2 = 'BRU'))
or ((c.o1 = 'XNA' or c.o2 = 'XNA') and (c.d1 = 'BRU' or c.d2 = 'BRU')) then 1 else 0 end as rank
from (select
*,
case when regexp_like(c0.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(c0.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when regexp_like(c0.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(c0.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as o1,
case when regexp_like(c0.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(c0.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when regexp_like(c0.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(c0.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as d1,

case when regexp_like(c0.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(c0.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when regexp_like(c0.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(c0.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as o2,
case when regexp_like(c0.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(c0.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
when regexp_like(c0.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(c0.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as d2
from diap01.mec_us_united_20056.dfa_activity as c0
where (cast(c0.click_time as date) between '2016-11-07' and '2016-12-31')
and not regexp_like( substring (c0.other_data,(instr(c0.other_data,'u3=') + 3),5),'mil.*','ib')
and c0.revenue <> 0
and c0.quantity <> 0
and c0.activity_type='ticke498'
and c0.activity_sub_type='unite820'
and c0.order_id=10505745
and c0.advertiser_id <> 0
) as c
left join
(
select
cast (t1.plc as varchar (4000)) as plc,
t1.page_id as page_id2,
t1.buy_id as buy_id,
t1.site_id as site_id2
from (select
p2.order_id as buy_id,
p2.site_id,
p2.page_id,
p2.site_placement as plc,
cast(p2.start_date as date) as thisDate,
row_number() over (partition by p2.order_id,p2.site_id,p2.page_id order by cast(p2.start_date as date)desc) as r1
from diap01.mec_us_united_20056.dfa_page as p2
) as t1
where t1.r1=1
) as j1
on c.page_id=j1.page_id2
and c.order_id=j1.buy_id
and c.site_ID=j1.site_id2

) as c9

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper(substring(c9.Other_Data,(INSTR(c9.Other_Data,'u3=')+3),3))= upper(Rates.Currency)
and cast(c9.Click_Time as date)=Rates.DATE
where c9.rank = 1

group by
cast(c9.Click_Time as date)
,c9.order_id
,c9.site_id
,c9.page_id);
commit;