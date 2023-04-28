# -*- coding: utf-8 -*-
# @Time    : 2023/4/28 22:38
# @Author  : HuangSir
# @FileName: sql.py
# @Software: PyCharm
# @Desc:模型监控

sql = """
(SELECT socre_range,count(DISTINCT a.BUSI_ID) 进件量,count(DISTINCT BID_ID) 放款量,count(DISTINCT BID_ID)/count(DISTINCT a.BUSI_ID) 放款率,if(sum(case when agr_pd1 = 1 then 1 else 0 end )=0,null,sum(case when agr_pd1 = 1 then 1 else 0 end )) as 到期订单,concat(round(sum(case when def_pd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 首逾,concat(round(sum(case when def_pd3 = 1 then 1  else 0 end )/sum(case when agr_pd3 = 1 then 1 else 0 end )*100,2),'%') as 'T+3首逾',concat(round(sum(case when cpd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 当前在逾
from(SELECT a.*,b.COMMENTS,c.*,case when RISK_RESULT<580 then '(-,580)'when RISK_RESULT<590 then '[580,590)'when RISK_RESULT<605 then '[590,605)'  when RISK_RESULT<640 then '[605,640)'when RISK_RESULT>=640 then '[640,+)'when FIRST_APP_STATUS=106 then '未跑模型分'end socre_range FROM loan_application a left JOIN (SELECT * from `third_risk`where risk_type='ABCASH_NEW_CUSTOMER_RISK_SCORE'  and JSON_EXTRACT(comments,'$.version') = '4.0') b on a.BUSI_ID=b.BUSI_ID   LEFT JOIN (SELECT 
	T1.BID_ID,-- 流水号
	T1.CUSTOMER_NO,-- 客户号
	T1.PLAN_STATUS,-- 1 未到期 2 已结清 3逾期 4 逾期后结清
	-- T1.PRODUCT_ID,-- 产品ID
	case when T4.CORP_NO='23' THEN 'ios' WHEN T4.CORP_NO='19' THEN 'android' else 'daichao' end as loan_plat,-- 贷款平台PRD06专用
	ROW_NUMBER()over(partition by CUSTOMER_NO ORDER BY T1.CREATE_TIME) as loan_rank,-- 第几笔贷款
	CASE WHEN T2.minpaytime IS NULL or T1.CREATE_TIME<T2.minpaytime then 'new'else 'old'end as cust_status,
	case when loans>1 then 1 else 0 end turnOld,-- 是否转换老客
	-- CASE WHEN DATEDIFF(DEADLINE,ORG_DEADLINE)=0 THEN '0' else'1' end extend, -- 0为未展期，1为展期
	-- DATEDIFF(DEADLINE,ORG_DEADLINE) extendDay, -- 展期天数
	T4.CREATE_TIME AS appl_time,-- 申请时间
	DATE_FORMAT(T4.CREATE_TIME,'%H')  appl_hour,
	T1.CREATE_TIME as loan_time,-- 贷款时间
	T1.DEADLINE,-- 实际还款日
	if(T1.PLAN_STATUS in(2,4),T1.REAL_REACH_TIME,NULL) settleDate,-- 结清时间
	if(T1.PLAN_STATUS in(3),DATEDIFF(CURRENT_DATE,T1.DEADLINE),null)AS currLateDay,-- 当前逾期天数
	CASE when T1.PLAN_STATUS =3 then DATEDIFF(CURRENT_DATE,T1.DEADLINE) when T1.PLAN_STATUS =4 then DATEDIFF(T1.REAL_REACH_TIME,T1.DEADLINE) ELSE NULL END as hisLateDay,-- 历史逾期天数
	-- T1.PENALTY_DAYS,
	T1.PRINCIPAL_AMOUNT,-- 贷款本金
	T1.REACH_PRINCIPAL,-- 已还本金
	T1.PENALTY,-- 罚息
	T1.REACH_PENALTY,-- 已还罚息
	T1.INTEREST_AMOUNT,-- 应还利息
	T1.REACH_INTEREST,-- 已还利息
	T1.LAFE_FEE,-- 滞纳金
	T1.REACH_LAFE_FEE,-- 已还滞纳金
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT as totalAmt,-- 总应还
	T1.REACH_AMOUNT,-- 总已还
	T1.WAIVER_AMOUNT,--  总减免
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT-T1.WAIVER_AMOUNT-T1.REACH_AMOUNT as resAmt,-- 总剩余
	-- 分母	
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 1)
      THEN 1
        ELSE 0 END AS agr_pd1,
  -- 分子
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 1)
								
      THEN 1        ELSE 0 END AS def_pd1,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd1,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 2)
      THEN 1
        ELSE 0 END AS agr_pd2,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 2)
								
      THEN 1        ELSE 0 END AS def_pd2,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd2,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 3)
      THEN 1
        ELSE 0 END AS agr_pd3,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 3)
								
      THEN 1        ELSE 0 END AS def_pd3,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd3									
FROM acc_borrow_plan T1 
LEFT JOIN (SELECT CUSTOMER_NO,min(REAL_REACH_TIME) minpaytime from acc_borrow_plan GROUP BY CUSTOMER_NO) T2 on T1.CUSTOMER_NO=T2.CUSTOMER_NO
LEFT JOIN (SELECT CUSTOMER_NO,count(DISTINCT BID_ID) loans from acc_borrow_plan GROUP BY CUSTOMER_NO) T3 on T1.CUSTOMER_NO=T3.CUSTOMER_NO
LEFT JOIN loan_application T4 on T1.BID_ID=T4.BUSI_ID
where T1.PLAN_STATUS<>7
) c on a.BUSI_ID=c.BID_ID 
where a.CREATE_TIME>='20230410')a
where (loan_rank=1 or loan_rank is null) and WHITE=0 and socre_range is not null
GROUP BY socre_range)
union
SELECT 'V4.0模型',count(DISTINCT a.BUSI_ID) 进件量,count(DISTINCT BID_ID) 放款量,count(DISTINCT BID_ID)/count(DISTINCT a.BUSI_ID) 放款率,if(sum(case when agr_pd1 = 1 then 1 else 0 end )=0,null,sum(case when agr_pd1 = 1 then 1 else 0 end )) as 到期订单,concat(round(sum(case when def_pd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 首逾,concat(round(sum(case when def_pd3 = 1 then 1  else 0 end )/sum(case when agr_pd3 = 1 then 1 else 0 end )*100,2),'%') as 'T+3首逾',concat(round(sum(case when cpd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 当前在逾
from(SELECT a.*,b.COMMENTS,c.*,case when RISK_RESULT<580 then '(-,580)'when RISK_RESULT<590 then '[580,590)'when RISK_RESULT<605 then '[590,605)'  when RISK_RESULT<640 then '[605,640)'when RISK_RESULT>=640 then '[640,+)'when FIRST_APP_STATUS=106 then '未跑模型分'end socre_range FROM loan_application a left JOIN (SELECT * from `third_risk`where risk_type='ABCASH_NEW_CUSTOMER_RISK_SCORE'  and JSON_EXTRACT(comments,'$.version') = '4.0') b on a.BUSI_ID=b.BUSI_ID   LEFT JOIN (SELECT 
	T1.BID_ID,-- 流水号
	T1.CUSTOMER_NO,-- 客户号
	T1.PLAN_STATUS,-- 1 未到期 2 已结清 3逾期 4 逾期后结清
	-- T1.PRODUCT_ID,-- 产品ID
	case when T4.CORP_NO='23' THEN 'ios' WHEN T4.CORP_NO='19' THEN 'android' else 'daichao' end as loan_plat,-- 贷款平台PRD06专用
	ROW_NUMBER()over(partition by CUSTOMER_NO ORDER BY T1.CREATE_TIME) as loan_rank,-- 第几笔贷款
	CASE WHEN T2.minpaytime IS NULL or T1.CREATE_TIME<T2.minpaytime then 'new'else 'old'end as cust_status,
	case when loans>1 then 1 else 0 end turnOld,-- 是否转换老客
	-- CASE WHEN DATEDIFF(DEADLINE,ORG_DEADLINE)=0 THEN '0' else'1' end extend, -- 0为未展期，1为展期
	-- DATEDIFF(DEADLINE,ORG_DEADLINE) extendDay, -- 展期天数
	T4.CREATE_TIME AS appl_time,-- 申请时间
	DATE_FORMAT(T4.CREATE_TIME,'%H')  appl_hour,
	T1.CREATE_TIME as loan_time,-- 贷款时间
	T1.DEADLINE,-- 实际还款日
	if(T1.PLAN_STATUS in(2,4),T1.REAL_REACH_TIME,NULL) settleDate,-- 结清时间
	if(T1.PLAN_STATUS in(3),DATEDIFF(CURRENT_DATE,T1.DEADLINE),null)AS currLateDay,-- 当前逾期天数
	CASE when T1.PLAN_STATUS =3 then DATEDIFF(CURRENT_DATE,T1.DEADLINE) when T1.PLAN_STATUS =4 then DATEDIFF(T1.REAL_REACH_TIME,T1.DEADLINE) ELSE NULL END as hisLateDay,-- 历史逾期天数
	-- T1.PENALTY_DAYS,
	T1.PRINCIPAL_AMOUNT,-- 贷款本金
	T1.REACH_PRINCIPAL,-- 已还本金
	T1.PENALTY,-- 罚息
	T1.REACH_PENALTY,-- 已还罚息
	T1.INTEREST_AMOUNT,-- 应还利息
	T1.REACH_INTEREST,-- 已还利息
	T1.LAFE_FEE,-- 滞纳金
	T1.REACH_LAFE_FEE,-- 已还滞纳金
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT as totalAmt,-- 总应还
	T1.REACH_AMOUNT,-- 总已还
	T1.WAIVER_AMOUNT,--  总减免
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT-T1.WAIVER_AMOUNT-T1.REACH_AMOUNT as resAmt,-- 总剩余
	-- 分母	
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 1)
      THEN 1
        ELSE 0 END AS agr_pd1,
  -- 分子
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 1)
								
      THEN 1        ELSE 0 END AS def_pd1,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd1,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 2)
      THEN 1
        ELSE 0 END AS agr_pd2,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 2)
								
      THEN 1        ELSE 0 END AS def_pd2,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd2,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 3)
      THEN 1
        ELSE 0 END AS agr_pd3,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 3)
								
      THEN 1        ELSE 0 END AS def_pd3,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd3						
FROM acc_borrow_plan T1 
LEFT JOIN (SELECT CUSTOMER_NO,min(REAL_REACH_TIME) minpaytime from acc_borrow_plan GROUP BY CUSTOMER_NO) T2 on T1.CUSTOMER_NO=T2.CUSTOMER_NO
LEFT JOIN (SELECT CUSTOMER_NO,count(DISTINCT BID_ID) loans from acc_borrow_plan GROUP BY CUSTOMER_NO) T3 on T1.CUSTOMER_NO=T3.CUSTOMER_NO
LEFT JOIN loan_application T4 on T1.BID_ID=T4.BUSI_ID
where T1.PLAN_STATUS<>7
) c on a.BUSI_ID=c.BID_ID 
where  a.CREATE_TIME>='20230410')a
where (loan_rank=1 or loan_rank is null) and WHITE=0 and socre_range<>'未跑模型分'
union
SELECT 'V4.0白名单',count(DISTINCT a.BUSI_ID) 进件量,count(DISTINCT BID_ID) 放款量,count(DISTINCT BID_ID)/count(DISTINCT a.BUSI_ID) 放款率,if(sum(case when agr_pd1 = 1 then 1 else 0 end )=0,null,sum(case when agr_pd1 = 1 then 1 else 0 end )) as 到期订单,concat(round(sum(case when def_pd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 首逾,concat(round(sum(case when def_pd3 = 1 then 1  else 0 end )/sum(case when agr_pd3 = 1 then 1 else 0 end )*100,2),'%') as 'T+3首逾',concat(round(sum(case when cpd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 当前在逾
from(SELECT a.*,b.COMMENTS,c.*,case when RISK_RESULT<580 then '(-,580)'when RISK_RESULT<590 then '[580,590)'when RISK_RESULT<605 then '[590,605)'  when RISK_RESULT<640 then '[605,640)'when RISK_RESULT>=640 then '[640,+)'when FIRST_APP_STATUS=106 then '未跑模型分'end socre_range FROM loan_application a left JOIN (SELECT * from `third_risk`where risk_type='ABCASH_NEW_CUSTOMER_RISK_SCORE'  and JSON_EXTRACT(comments,'$.version') = '4.0') b on a.BUSI_ID=b.BUSI_ID   LEFT JOIN (SELECT 
	T1.BID_ID,-- 流水号
	T1.CUSTOMER_NO,-- 客户号
	T1.PLAN_STATUS,-- 1 未到期 2 已结清 3逾期 4 逾期后结清
	-- T1.PRODUCT_ID,-- 产品ID
	case when T4.CORP_NO='23' THEN 'ios' WHEN T4.CORP_NO='19' THEN 'android' else 'daichao' end as loan_plat,-- 贷款平台PRD06专用
	ROW_NUMBER()over(partition by CUSTOMER_NO ORDER BY T1.CREATE_TIME) as loan_rank,-- 第几笔贷款
	CASE WHEN T2.minpaytime IS NULL or T1.CREATE_TIME<T2.minpaytime then 'new'else 'old'end as cust_status,
	case when loans>1 then 1 else 0 end turnOld,-- 是否转换老客
	-- CASE WHEN DATEDIFF(DEADLINE,ORG_DEADLINE)=0 THEN '0' else'1' end extend, -- 0为未展期，1为展期
	-- DATEDIFF(DEADLINE,ORG_DEADLINE) extendDay, -- 展期天数
	T4.CREATE_TIME AS appl_time,-- 申请时间
	DATE_FORMAT(T4.CREATE_TIME,'%H')  appl_hour,
	T1.CREATE_TIME as loan_time,-- 贷款时间
	T1.DEADLINE,-- 实际还款日
	if(T1.PLAN_STATUS in(2,4),T1.REAL_REACH_TIME,NULL) settleDate,-- 结清时间
	if(T1.PLAN_STATUS in(3),DATEDIFF(CURRENT_DATE,T1.DEADLINE),null)AS currLateDay,-- 当前逾期天数
	CASE when T1.PLAN_STATUS =3 then DATEDIFF(CURRENT_DATE,T1.DEADLINE) when T1.PLAN_STATUS =4 then DATEDIFF(T1.REAL_REACH_TIME,T1.DEADLINE) ELSE NULL END as hisLateDay,-- 历史逾期天数
	-- T1.PENALTY_DAYS,
	T1.PRINCIPAL_AMOUNT,-- 贷款本金
	T1.REACH_PRINCIPAL,-- 已还本金
	T1.PENALTY,-- 罚息
	T1.REACH_PENALTY,-- 已还罚息
	T1.INTEREST_AMOUNT,-- 应还利息
	T1.REACH_INTEREST,-- 已还利息
	T1.LAFE_FEE,-- 滞纳金
	T1.REACH_LAFE_FEE,-- 已还滞纳金
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT as totalAmt,-- 总应还
	T1.REACH_AMOUNT,-- 总已还
	T1.WAIVER_AMOUNT,--  总减免
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT-T1.WAIVER_AMOUNT-T1.REACH_AMOUNT as resAmt,-- 总剩余
	-- 分母	
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 1)
      THEN 1
        ELSE 0 END AS agr_pd1,
  -- 分子
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 1)
								
      THEN 1        ELSE 0 END AS def_pd1,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd1,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 2)
      THEN 1
        ELSE 0 END AS agr_pd2,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 2)
								
      THEN 1        ELSE 0 END AS def_pd2,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd2,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 3)
      THEN 1
        ELSE 0 END AS agr_pd3,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 3)
								
      THEN 1        ELSE 0 END AS def_pd3,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd3						
FROM acc_borrow_plan T1 
LEFT JOIN (SELECT CUSTOMER_NO,min(REAL_REACH_TIME) minpaytime from acc_borrow_plan GROUP BY CUSTOMER_NO) T2 on T1.CUSTOMER_NO=T2.CUSTOMER_NO
LEFT JOIN (SELECT CUSTOMER_NO,count(DISTINCT BID_ID) loans from acc_borrow_plan GROUP BY CUSTOMER_NO) T3 on T1.CUSTOMER_NO=T3.CUSTOMER_NO
LEFT JOIN loan_application T4 on T1.BID_ID=T4.BUSI_ID
where T1.PLAN_STATUS<>7
) c on a.BUSI_ID=c.BID_ID 
where  a.CREATE_TIME>='20230410')a
where (loan_rank=1 or loan_rank is null) and WHITE=1 and socre_range<>'未跑模型分'
union
SELECT 'V4.0整体',count(DISTINCT a.BUSI_ID) 进件量,count(DISTINCT BID_ID) 放款量,count(DISTINCT BID_ID)/count(DISTINCT a.BUSI_ID) 放款率,if(sum(case when agr_pd1 = 1 then 1 else 0 end )=0,null,sum(case when agr_pd1 = 1 then 1 else 0 end )) as 到期订单,concat(round(sum(case when def_pd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 首逾,concat(round(sum(case when def_pd3 = 1 then 1  else 0 end )/sum(case when agr_pd3 = 1 then 1 else 0 end )*100,2),'%') as 'T+3首逾',concat(round(sum(case when cpd1 = 1 then 1  else 0 end )/sum(case when agr_pd1 = 1 then 1 else 0 end )*100,2),'%') as 当前在逾
from(SELECT a.*,b.COMMENTS,c.*,case when RISK_RESULT<580 then '(-,580)'when RISK_RESULT<590 then '[580,590)'when RISK_RESULT<605 then '[590,605)'  when RISK_RESULT<640 then '[605,640)'when RISK_RESULT>=640 then '[640,+)'when FIRST_APP_STATUS=106 then '未跑模型分'end socre_range FROM loan_application a left JOIN (SELECT * from `third_risk`where risk_type='ABCASH_NEW_CUSTOMER_RISK_SCORE'  and JSON_EXTRACT(comments,'$.version') = '4.0') b on a.BUSI_ID=b.BUSI_ID   LEFT JOIN (SELECT 
	T1.BID_ID,-- 流水号
	T1.CUSTOMER_NO,-- 客户号
	T1.PLAN_STATUS,-- 1 未到期 2 已结清 3逾期 4 逾期后结清
	-- T1.PRODUCT_ID,-- 产品ID
	case when T4.CORP_NO='23' THEN 'ios' WHEN T4.CORP_NO='19' THEN 'android' else 'daichao' end as loan_plat,-- 贷款平台PRD06专用
	ROW_NUMBER()over(partition by CUSTOMER_NO ORDER BY T1.CREATE_TIME) as loan_rank,-- 第几笔贷款
	CASE WHEN T2.minpaytime IS NULL or T1.CREATE_TIME<T2.minpaytime then 'new'else 'old'end as cust_status,
	case when loans>1 then 1 else 0 end turnOld,-- 是否转换老客
	-- CASE WHEN DATEDIFF(DEADLINE,ORG_DEADLINE)=0 THEN '0' else'1' end extend, -- 0为未展期，1为展期
	-- DATEDIFF(DEADLINE,ORG_DEADLINE) extendDay, -- 展期天数
	T4.CREATE_TIME AS appl_time,-- 申请时间
	DATE_FORMAT(T4.CREATE_TIME,'%H')  appl_hour,
	T1.CREATE_TIME as loan_time,-- 贷款时间
	T1.DEADLINE,-- 实际还款日
	if(T1.PLAN_STATUS in(2,4),T1.REAL_REACH_TIME,NULL) settleDate,-- 结清时间
	if(T1.PLAN_STATUS in(3),DATEDIFF(CURRENT_DATE,T1.DEADLINE),null)AS currLateDay,-- 当前逾期天数
	CASE when T1.PLAN_STATUS =3 then DATEDIFF(CURRENT_DATE,T1.DEADLINE) when T1.PLAN_STATUS =4 then DATEDIFF(T1.REAL_REACH_TIME,T1.DEADLINE) ELSE NULL END as hisLateDay,-- 历史逾期天数
	-- T1.PENALTY_DAYS,
	T1.PRINCIPAL_AMOUNT,-- 贷款本金
	T1.REACH_PRINCIPAL,-- 已还本金
	T1.PENALTY,-- 罚息
	T1.REACH_PENALTY,-- 已还罚息
	T1.INTEREST_AMOUNT,-- 应还利息
	T1.REACH_INTEREST,-- 已还利息
	T1.LAFE_FEE,-- 滞纳金
	T1.REACH_LAFE_FEE,-- 已还滞纳金
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT as totalAmt,-- 总应还
	T1.REACH_AMOUNT,-- 总已还
	T1.WAIVER_AMOUNT,--  总减免
	T1.PRINCIPAL_AMOUNT+T1.PENALTY+T1.LAFE_FEE+T1.INTEREST_AMOUNT-T1.WAIVER_AMOUNT-T1.REACH_AMOUNT as resAmt,-- 总剩余
	-- 分母	
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 1)
      THEN 1
        ELSE 0 END AS agr_pd1,
  -- 分子
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 1)
								
      THEN 1        ELSE 0 END AS def_pd1,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 1) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd1,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 2)
      THEN 1
        ELSE 0 END AS agr_pd2,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 2)
								
      THEN 1        ELSE 0 END AS def_pd2,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 2) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd2,
	--
	CASE WHEN ( datediff(CURDATE(),T1.DEADLINE)>= 3)
      THEN 1
        ELSE 0 END AS agr_pd3,
  CASE WHEN ((datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))) OR (T1.PLAN_STATUS IN(4) AND datediff(T1.REAL_REACH_TIME,T1.DEADLINE)>= 3)
								
      THEN 1        ELSE 0 END AS def_pd3,
			
	CASE WHEN (datediff(CURDATE(),T1.DEADLINE)>= 3) AND (T1.PLAN_STATUS IN(3))
								
      THEN 1        ELSE 0 END AS cpd3						
FROM acc_borrow_plan T1 
LEFT JOIN (SELECT CUSTOMER_NO,min(REAL_REACH_TIME) minpaytime from acc_borrow_plan GROUP BY CUSTOMER_NO) T2 on T1.CUSTOMER_NO=T2.CUSTOMER_NO
LEFT JOIN (SELECT CUSTOMER_NO,count(DISTINCT BID_ID) loans from acc_borrow_plan GROUP BY CUSTOMER_NO) T3 on T1.CUSTOMER_NO=T3.CUSTOMER_NO
LEFT JOIN loan_application T4 on T1.BID_ID=T4.BUSI_ID
where T1.PLAN_STATUS<>7
) c on a.BUSI_ID=c.BID_ID 
where  a.CREATE_TIME>='20230410')a
where (loan_rank=1 or loan_rank is null)  and socre_range<>'未跑模型分'
"""