/***** DELETE DATA FROM HR_TXNS_CALC_COM_MERGE_ALL FOR SELECTED PAY PERIODS *****/

delete 
from 
 FINCOMM.COM_hr_txns_calc_com_merge_all m
where
 m.pay_period_id IN (SELECT P.PP_ID
                     FROM
                      FINCOMM.COM_HR_JEID_PP P
                     WHERE
                      P.TO_BE_PROCESSED = 'Y' AND
                      P.PROCESSED = 'N');
COMMIT;

/***** LOAD 'NCCP' DATA TO HR_TXNS_CALC_COM_MERGE_ALL *****/
insert into  fincomm.com_hr_txns_calc_com_merge_all
 (pay_period_id, kgen_payeeid, rep_type, pp_active, css_payeeid,
  canvass_id, customer_id, cust_type, prod_type, product_code,
  product_issue_num, thryv_ind, dar_ind, cancel_ind, cancel_match, cancel_pay_period,
  ni_amt, pi_amt, source, calc_new_com, calc_renew_com, 
  com_amt, total_pp_incentive_amt, payment_amt, stage_id, ph_id, 
  com_ind, thld_met_ind, debit_proration_ind, cumul_com_ind_total, cumul_com_ind_pos_tot, 
  cumul_com_ind_neg_tot, cumul_com_total, cumul_com_pos_tot, cumul_com_neg_tot)

select
 s1.pay_period_id,
 s1.kgen_payeeid,
 s1.rep_type,
 s1.pp_active,
 s1.css_payeeid,
 s1.canvass_id,
 s1.customer_id,
 s1.cust_type,
 s1.prod_type,
 s1.product_code,
 s1.product_issue_num,
 s1.thryv_ind,
 s1.dar_ind,
 s1.cancel_ind,
 s1.cancel_match,
 s1.cancel_pay_period,
 s1.ni_amt,
 s1.pi_amt,
 s1.source,
 s1.calc_new_com,
 s1.calc_renew_com,
 s1.com_amt,
 s1.total_pp_incentive_amt,
 s1.payment_amt,
 s1.stage_id,
 s1.ph_id,
 s1.com_ind, 
 s1.thld_met_ind,
 s1.debit_proration_ind,
 sum(s1.calc_new_com+s1.calc_renew_com) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid,
       s1.com_ind) as cumul_com_ind_total,
 sum(case when s1.calc_new_com > 0 then s1.calc_new_com else 0 end +
     case when s1.calc_renew_com > 0 then s1.calc_renew_com else 0 end) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid,
       s1.com_ind) as cumul_com_ind_pos_tot,
 sum(case when s1.calc_new_com < 0 then s1.calc_new_com else 0 end +
     case when s1.calc_renew_com < 0 then s1.calc_renew_com else 0 end) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid,
       s1.com_ind) as cumul_com_ind_neg_tot,
 sum(s1.calc_new_com+s1.calc_renew_com) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid) as cumul_com_total,
 sum(case when s1.calc_new_com > 0 then s1.calc_new_com else 0 end +
     case when s1.calc_renew_com > 0 then s1.calc_renew_com else 0 end) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid) as cumul_com_pos_tot,
 sum(case when s1.calc_new_com < 0 then s1.calc_new_com else 0 end +
     case when s1.calc_renew_com < 0 then s1.calc_renew_com else 0 end) over 
    (partition by
       s1.pay_period_id,
       s1.kgen_payeeid) as cumul_com_neg_tot                
       
from
 ( 
    select
     p.pay_period_id,
     p.rep_num as kgen_payeeid,
     'NCCP' as rep_type,
     p.pp_active,
     '' as css_payeeid,
     '' as canvass_id,
     s.customer_id,
     s.cust_type,
     s.prod_type,
     s.product_code,
     s.product_issue_num,
     s.dar_ind,
     s.thryv_ind,
     s.cancel_ind,
     s.cancel_match,
     s.cancel_pay_period,
     s.ni_amt,
     s.pi_amt,
     s.source,

     /***** New Customer Commission *****/
     case when s.product_type != 'SEM' then 
               round((s.calc_new_amt*(p.newnon_rate-p.renewal_rate)),2) else 0 end +
     case when s.product_type != 'SEM' then 
               round((s.calc_increase_amt*(p.increase_rate-p.renewal_rate)),2) else 0 end +
     case when s.product_type = 'SEM' then 
               round((s.calc_new_amt*(p.sem_nni_rate-p.renewal_rate)),2) else 0 end +
     case when s.product_type = 'SEM' then 
               round((s.calc_increase_amt*(p.sem_nni_rate-p.renewal_rate)),2) else 0 end          
         as calc_new_com,            
     /***** Renew Customer Commission *****/    
     NVL(round((s.calc_new_amt*p.renewal_rate),2) +
     round((s.calc_renewal_amt*p.renewal_rate),2)+
     round((s.calc_increase_amt*p.renewal_rate),2),0) 
         as calc_renew_com,
     p.com_amt,
     p.total_com_amt+p.sem_total_com+p.debit_pro_adj_com_amt as total_pp_incentive_amt,
     case when p.payment_amt+p.offcycle_payments_amt = 0 then x.ern_amt 
          else p.payment_amt+p.offcycle_payments_amt end as payment_amt,
     s.rowid as stage_id,
     p.rowid as ph_id,
     s.com_ind,
     '' AS thld_met_ind,
     '' AS debit_proration_ind
    from 
     fincomm.fincomm_ph_nccp_pp_detail p
      left join fincomm.com_hr_txns_stage_all s
        on 
         p.pay_period_id = s.pay_period_id and
         p.rep_num = s.kgen_payeeid
      inner join 
       (select 
         pp.pay_period_id,
         i.employee_id,
         r.kgen_payeeid,
         sum(i.ern_amt) as ern_amt
        from 
         FINCOMM.FINCOMM_PP_INCENTIVE_DATA i
         inner join fincomm.fincomm_pay_period_13mth pp
          on i.ern_pay_period_dte = pp.cheque_date
         inner join FINCOMM.FINCOMM_SCP_REP_LIST r
          on pp.pay_period_id = r.pay_period_id and
             i.employee_id = r.clock_id          
        where
         i.ERN_CDE IN ('COMSC','COMBC') and
         pp.PAY_PERIOD_ID = (SELECT MAX(P.PP_ID) AS PP_ID
                             FROM
                              FINCOMM.COM_HR_JEID_PP P
                             WHERE
                              P.TO_BE_PROCESSED = 'Y' AND
                              P.PROCESSED = 'N')
        group by
         pp.pay_period_id,
         i.employee_id,
         r.kgen_payeeid
       ) x
       on x.pay_period_id = p.pay_period_id and
          x.kgen_payeeid = p.rep_num
      where
       NVL(S.SOURCE,'X') != 'C' and
       p.is_last = 1 and
       P.PAY_PERIOD_ID = (SELECT MAX(P.PP_ID) AS PP_ID
                     FROM
                      FINCOMM.COM_HR_JEID_PP P
                     WHERE
                      P.TO_BE_PROCESSED = 'Y' AND
                      P.PROCESSED = 'N')
 ) s1;
COMMIT;

/***** UPDATE MERGE FIELDS CALC_NEW_FINAL_COM AND CALC_RENEW_FINAL_COM FOR NCCP AND COLN *****/
/***** THIS SECTION HAS BEEN MODIFIED DUE TO AN ALLOCATION AMPLIFICATION ISSUE 06-09-2021 TICKET INC-682830 *****/
update fincomm.com_hr_txns_calc_com_merge_all m
set 
  m.CALC_NEW_FINAL_COM =
   case when m.payment_amt = 0 then 0
        when m.total_pp_incentive_amt = 0 then 0
        when m.thld_met_ind = 'Y' and m.debit_proration_ind !='N' and
             m.com_ind = 2 then 0
        when m.thld_met_ind = 'Y' and m.debit_proration_ind !='N' and
             m.com_ind = 1 then
               case when m.cumul_com_ind_total = 0 then 0
                    when m.cumul_com_ind_total > 1 then 
                            round(m.total_pp_incentive_amt*(m.calc_new_com/(m.cumul_com_ind_total)),2)
                    when m.calc_new_com < 0 then m.calc_new_com
                    when m.calc_new_com > 0 then
                         round(m.calc_new_com/m.cumul_com_ind_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_ind_neg_tot),2) 
                    else 0 end  
        when m.com_amt = 0 then 0       
        when m.cumul_com_total = 0 then 0          
        /***** Below is new for amplification issue 06-09-2021 *****/            
        when  abs((m.total_pp_incentive_amt-m.cumul_com_total)/m.cumul_com_total) > 1 and m.total_pp_incentive_amt > 0 
              and m.cumul_com_pos_tot > 0 then
               case when m.calc_new_com < 0 then m.calc_new_com
                    when m.calc_new_com > 0 then
                         round(m.calc_new_com/m.cumul_com_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_neg_tot),2) 
                    else 0 end  
        /***** Below is new for amplification issue 06-09-2021*****/
        when  abs((m.total_pp_incentive_amt-m.cumul_com_total)/m.cumul_com_total) > 1 and m.total_pp_incentive_amt < 0 
              and m.cumul_com_neg_tot < 0 then
               case when m.calc_new_com > 0 then m.calc_new_com
                    when m.calc_new_com < 0 then
                         round(m.calc_new_com/m.cumul_com_neg_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_pos_tot),2) 
                    else 0 end       
        when m.com_amt > 1 or m.com_amt < -1 then 
                 round(m.total_pp_incentive_amt*(m.calc_new_com/(m.cumul_com_total)),2) 
        when m.com_amt > 0 and m.cumul_com_total > 0 then
               case when m.calc_new_com < 0 then m.calc_new_com
                    when m.calc_new_com > 0 then
                         round(m.calc_new_com/m.cumul_com_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_neg_tot),2) 
                    else 0 end  
        when m.com_amt < 0 and m.cumul_com_total < 0 then
               case when m.calc_new_com > 0 then m.calc_new_com
                    when m.calc_new_com < 0 then
                         round(m.calc_new_com/m.cumul_com_neg_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_pos_tot),2) 
                    else 0 end       
    else 0 end,

 m.CALC_RENEW_FINAL_COM = 
   case when m.payment_amt = 0 then 0
        when m.total_pp_incentive_amt = 0 then 0
        when m.thld_met_ind = 'Y' and m.debit_proration_ind !='N' and
             m.com_ind = 2 then 0
        when m.thld_met_ind = 'Y' and m.debit_proration_ind !='N' and
             m.com_ind = 1 then
               case when m.cumul_com_ind_total = 0 then 0
                    when m.cumul_com_ind_total > 1 then 
                            round(m.total_pp_incentive_amt*(m.calc_renew_com/(m.cumul_com_ind_total)),2)
                    when m.calc_renew_com < 0 then m.calc_renew_com
                    when m.calc_renew_com > 0 then
                         round(m.calc_renew_com/m.cumul_com_ind_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_ind_neg_tot),2) 
                    else 0 end  
        when m.com_amt = 0 then 0       
        when m.cumul_com_total = 0 then 0          
        /***** Below is new for amplification issue 06-09-2021 *****/            
        when  abs((m.total_pp_incentive_amt-m.cumul_com_total)/m.cumul_com_total) > 1 and m.total_pp_incentive_amt > 0 
              and m.cumul_com_pos_tot > 0 then
               case when m.calc_renew_com < 0 then m.calc_renew_com
                    when m.calc_renew_com > 0 then
                         round(m.calc_renew_com/m.cumul_com_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_neg_tot),2) 
                    else 0 end  
        /***** Below is new for amplification issue 06-08-2021 *****/
        when  abs((m.total_pp_incentive_amt-m.cumul_com_total)/m.cumul_com_total) > 1 and m.total_pp_incentive_amt < 0 
              and m.cumul_com_neg_tot < 0 then
               case when m.calc_renew_com > 0 then m.calc_renew_com
                    when m.calc_renew_com < 0 then
                         round(m.calc_renew_com/m.cumul_com_neg_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_pos_tot),2) 
                    else 0 end       
        
        when m.com_amt > 1 or m.com_amt < -1 then 
                 round(m.total_pp_incentive_amt*(m.calc_renew_com/(m.cumul_com_total)),2) 
        when m.com_amt > 0 and m.cumul_com_total > 0 then
               case when m.calc_renew_com < 0 then m.calc_renew_com
                    when m.calc_renew_com > 0 then
                         round(m.calc_renew_com/m.cumul_com_pos_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_neg_tot),2) 
                    else 0 end  
        when m.com_amt < 0 and m.cumul_com_total < 0 then
               case when m.calc_renew_com > 0 then m.calc_renew_com
                    when m.calc_renew_com < 0 then
                         round(m.calc_renew_com/m.cumul_com_neg_tot*
                              (m.total_pp_incentive_amt-m.cumul_com_pos_tot),2) 
                    else 0 end       
    else 0 end
where
 m.rep_type in('NCCP') and
 m.pay_period_id IN (SELECT max(P.PP_ID) as pp_id
                     FROM
                      FINCOMM.COM_HR_JEID_PP P
                     WHERE
                      P.TO_BE_PROCESSED = 'Y' AND
                      P.PROCESSED = 'N');
COMMIT;

 
 
 
