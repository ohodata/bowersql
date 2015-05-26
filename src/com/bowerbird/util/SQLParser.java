package com.bowerbird.util;
/*目前支持sql的语法
 * 查询表：只支持单表，不支持多表和子查询
 * 查询字段：只支持单一字段及单一字段的聚合函数，不支持表达式、子查询
 * 查询过滤条件：只支持表达式，不支持子查询
 * 查询分组：只支持在查询字段中出现的字段
 * Having条件：只支持形式f(x)>10，且f(x)必须为查询字段
 * 查询排序：只支持在查询字段中出现的字段
 * 查询条数：完全支持
 * 支持的聚合函数：sum,count,count distinct,avg,min,max,stddev_pop/stddev_samp,var_pop/var_samp,covar_pop/covar/samp,corr
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr.Option;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

public class SQLParser {
	public List<HashMap<String,String>> selectColumns=new ArrayList<HashMap<String,String>>();
	public List<HashMap<String,String>> fromTables=new ArrayList<HashMap<String,String>>();
	public List<HashMap<String,String>> groupColumns=new ArrayList<HashMap<String,String>>();
	public List<HashMap<String,String>> havingColumns=new ArrayList<HashMap<String,String>>();
	public List<HashMap<String,String>> orderColumns=new ArrayList<HashMap<String,String>>();
	public List<HashMap<String,String>> whereColumns=new ArrayList<HashMap<String,String>>();
	public HashMap<String,Number> selectLimit=new HashMap<String,Number>();
	public boolean hasGroup=false;
	public boolean hasCountDistinct=false;
	public String origin_sql="";
	public String sql_id="";
	public String getTableType(String tab_name)
	{
		if(tab_name.equalsIgnoreCase("test"))
		{
			return "fact";
		}
		return "dim";
	}
	public SQLParser(String id,String sql) {
		origin_sql=sql;
		sql_id=id;
		int i=0;
		int j=0;
		MySqlStatementParser parser = new MySqlStatementParser(sql);
		List<SQLStatement> stmtList = parser.parseStatementList();
		SQLStatement stmt = stmtList.get(0);

		SQLSelect select = ((SQLSelectStatement) stmt).getSelect();
		MySqlSelectQueryBlock query =(MySqlSelectQueryBlock)select.getQuery();
		
		SQLExpr expr;
		String alias;
		HashMap<String,String> map;
		//获得table
		SQLTableSource f=query.getFrom();
		if(f instanceof SQLJoinTableSource)
		{
			SQLJoinTableSource from = (SQLJoinTableSource)f;
			SQLExprTableSource tab_left=(SQLExprTableSource)from.getLeft();
			SQLExpr expr_left=tab_left.getExpr();
			String alias_left=tab_left.getAlias();
			String tab_name_left=expr_left.toString();
			SQLExprTableSource tab_right=(SQLExprTableSource)from.getRight();
			SQLExpr expr_right=tab_left.getExpr();
			String alias_right=tab_left.getAlias();
			String tab_name_right=expr_right.toString();
			String join_type=from.getJoinType().name;
		}
		else if(f instanceof SQLExprTableSource)
		{
			SQLExprTableSource from = (SQLExprTableSource)f;
			//String tab_name=(String);
			expr=from.getExpr();
			alias=from.getAlias();
			String tab_name=expr.toString();
			map=new HashMap<String,String>();
			map.put("name", tab_name);
			map.put("alias", alias);
			if(map.get("alias")==null || map.get("alias").equals("null"))
			{
				map.put("alias", map.get("name"));
			}
			fromTables.add(map);
		}
		else
		{
			System.out.println("from type not support: "+f.toString()+" is "+f.getClass());
		}
		
		//System.out.println("table: "+ map.get("name")+" "+ map.get("alias"));

		//获得select
		List<SQLSelectItem> items=query.getSelectList();
		String select_str="";
		SQLSelectItem item;
		for(i=0;i<items.size();i++)
		{
			map=new HashMap<String,String>();
			if(i!=0) select_str+=",";
			item=items.get(i);
			map.put("alias", item.getAlias());
			expr=item.getExpr();
			if(expr instanceof SQLAggregateExpr)
			{
				String method_name=((SQLAggregateExpr)expr).getMethodName();
				
				map.put("type", "measure");
				map.put("method", method_name);
				Option option=((SQLAggregateExpr)expr).getOption();
				String option_name="";
				if(option!=null)
				{
					option_name=option.name();
					map.put("option", option_name);
				}
				
				
				if(method_name.equalsIgnoreCase("count") && option_name.equalsIgnoreCase("distinct"))
				{
					hasCountDistinct=true;
				}
				select_str+=method_name+'(';
				List<SQLExpr> arrs=((SQLAggregateExpr)expr).getArguments();
				String arg="";
				for(j=0;j<arrs.size();j++)
				{
					if(j!=0) 
					{
						select_str+=",";
						arg+=",";
					}
					expr=arrs.get(j);
					if(expr  instanceof SQLPropertyExpr)
					{
						map.put("name", ((SQLPropertyExpr) expr).getName());
						map.put("owner",((SQLIdentifierExpr)((SQLPropertyExpr) expr).getOwner()).getName());
					}
					else if(expr  instanceof SQLIdentifierExpr)
					{
						map.put("name", ((SQLIdentifierExpr) expr).getName());
						map.put("owner",null);
					}
					else if(expr  instanceof SQLAllColumnExpr)
					{
						map.put("name", "*");
						map.put("owner",null);
					}
					else if(expr instanceof SQLBinaryOpExpr)
					{
						map.put("name", ((SQLBinaryOpExpr) expr).getLeft().toString()+((SQLBinaryOpExpr) expr).getOperator().name+((SQLBinaryOpExpr) expr).getRight().toString());
						map.put("owner",null);
					}
					else if(expr instanceof SQLMethodInvokeExpr)
					{
						map.put("name", ((SQLMethodInvokeExpr) expr).toString());
						map.put("owner",null);
					}
					else
					{
						System.out.println("select aggr column not support: "+expr.toString()+" is "+expr.getClass());
					}

					select_str+=map.get("owner")+"."+map.get("name");

				}
				if(map.get("alias")==null || map.get("alias").equals("null"))
				{
					if(map.get("name").equals("*"))
					{
						map.put("alias", map.get("method"));
					}
					else
					{
						String  regEx ="[^a-zA-Z0-9]";
						Pattern   p   =   Pattern.compile(regEx);     
					    Matcher   m   =   p.matcher(map.get("name"));     
						map.put("alias",m.replaceAll("_").trim());
					}

				}
				selectColumns.add(map);
				select_str+=")";		
			}
			else if(expr instanceof SQLMethodInvokeExpr)
			{
				String method_name=((SQLMethodInvokeExpr) expr).getMethodName();
				map.put("type", "measure");
				map.put("method", method_name);
				
				List<SQLExpr> arrs=((SQLMethodInvokeExpr)expr).getParameters();
				for(j=0;j<arrs.size();j++)
				{
					expr=arrs.get(j);
					if(expr  instanceof SQLPropertyExpr)
					{
						map.put("name", ((SQLPropertyExpr) expr).getName());
						map.put("owner",((SQLIdentifierExpr)((SQLPropertyExpr) expr).getOwner()).getName());
					}
					else if(expr  instanceof SQLIdentifierExpr)
					{
						map.put("name", ((SQLIdentifierExpr) expr).getName());
						map.put("owner",null);
					}
					else if(expr  instanceof SQLAllColumnExpr)
					{
						map.put("name", "*");
						map.put("owner",null);
					}
					else
					{
						System.out.println("select method column not support: "+expr.toString()+" is "+expr.getClass());
					}

					select_str+=map.get("owner")+"."+map.get("name");

				}
				if(map.get("alias")==null || map.get("alias").equals("null"))
				{
					if(map.get("name").equals("*"))
					{
						map.put("alias", map.get("method"));
					}
					else
					{
						String  regEx ="[^a-zA-Z0-9]";
						Pattern   p   =   Pattern.compile(regEx);     
					    Matcher   m   =   p.matcher(map.get("name"));     
						map.put("alias",m.replaceAll("_").trim());
						
					}

				}
				selectColumns.add(map);
			}
			else if(expr instanceof SQLPropertyExpr)
			{
				map.put("type", "dimension");
				map.put("name", ((SQLPropertyExpr) expr).getName());
				if(map.get("alias")==null || map.get("alias").equals("null"))
				{
					String  regEx ="[^a-zA-Z0-9]";
					Pattern   p   =   Pattern.compile(regEx);     
				    Matcher   m   =   p.matcher(map.get("name"));     
					map.put("alias",m.replaceAll("_").trim());
				}
				map.put("owner", ((SQLIdentifierExpr)((SQLPropertyExpr) expr).getOwner()).getName());
				selectColumns.add(map);
				select_str+=map.get("owner")+"."+map.get("name");
			}
			else if(expr instanceof SQLIdentifierExpr)
			{

				map.put("name", ((SQLIdentifierExpr) expr).getName());
				map.put("owner", null);
				map.put("type", "dimension");
				if(map.get("alias")==null  || map.get("alias").equals("null"))
				{
					String  regEx ="[^a-zA-Z0-9]";
					Pattern   p   =   Pattern.compile(regEx);     
				    Matcher   m   =   p.matcher(map.get("name"));     
					map.put("alias",m.replaceAll("_").trim());
				}
				selectColumns.add(map);
				select_str+=map.get("name");
			}
			else
			{
				System.out.println("select column not support:"+expr.toString()+" is "+expr.getClass());
			}

		}
		//System.out.println("select:"+select_str);

		//获得group
		SQLSelectGroupByClause gclause=query.getGroupBy();
		if(gclause!=null)
		{
			List<SQLExpr> expr_arr=gclause.getItems();
			String group_str="";
			for(i=0;i<expr_arr.size();i++)
			{
				map=new HashMap<String,String>();
				if(i!=0) select_str+=",";
				expr=expr_arr.get(i);
				if(expr instanceof SQLPropertyExpr)
				{
					map.put("name", ((SQLPropertyExpr) expr).getName());
					map.put("owner", ((SQLIdentifierExpr)((SQLPropertyExpr) expr).getOwner()).getName());
				}
				else  if(expr instanceof SQLIdentifierExpr)
				{
					map.put("name", ((SQLIdentifierExpr) expr).getName());
				}
				else
				{
					System.out.println("group column not support:"+expr.toString()+" is "+expr.getClass());
				}
				groupColumns.add(map);
				group_str+=map.get("owner")+"."+map.get("name");
				hasGroup=true;
			}
			expr=gclause.getHaving();
			if(expr!=null)
			{
				map=new HashMap<String,String>();
				if(expr instanceof SQLBinaryOpExpr)
				{
					SQLExpr left=((SQLBinaryOpExpr)expr).left;
					SQLBinaryOperator oper=((SQLBinaryOpExpr)expr).operator;
					SQLExpr right=((SQLBinaryOpExpr)expr).right;
					if(left instanceof SQLAggregateExpr)
					{
						String name="";
						name+=((SQLAggregateExpr)left).getMethodName()+"(";
						Option option=((SQLAggregateExpr)left).getOption();
						String option_name="";
						if(option!=null)
						{
							name+=option.name();
						}
						
						List<SQLExpr> arrs=((SQLAggregateExpr)left).getArguments();
						for(j=0;j<arrs.size();j++)
						{
							expr=arrs.get(j);
							if(expr  instanceof SQLPropertyExpr)
							{
								name+=((SQLIdentifierExpr)((SQLPropertyExpr) expr).getOwner()).getName()+"."+((SQLPropertyExpr) expr).getName();
							}
							else if(expr  instanceof SQLIdentifierExpr)
							{
								name+=((SQLIdentifierExpr) expr).getName();
							}
							else if(expr  instanceof SQLAllColumnExpr)
							{
								name+="*";
							}
							else
							{
								System.out.println("having left aggr column not support:"+expr.toString()+" is "+expr.getClass());
							}

							

						}
						name+=")";
						map.put("left", name);
					}
					else
					{
						System.out.println("having left column not support:"+expr.toString()+" is "+expr.getClass());
					}
					map.put("oper", oper.name);
					if(right instanceof SQLIntegerExpr)
					{
						map.put("right", ((SQLIntegerExpr) right).getNumber()+"");
					}
					else if(right instanceof SQLNumberExpr)
					{
						map.put("right", ((SQLNumberExpr) right).getNumber()+"");
					}
					else
					{
						System.out.println("having right column not support:"+expr.toString()+" is "+expr.getClass());
					}
					havingColumns.add(map);
				}
				else
				{
					System.out.println("having column not support:"+expr.toString()+" is "+expr.getClass());
				}
			}
			

			// System.out.println("group:"+group_str);
		}


		//获得order
		SQLOrderBy orderby=query.getOrderBy();
		if(orderby!=null)
		{
			List<SQLSelectOrderByItem> order_arr=orderby.getItems();
			String order_str="";
			for(i=0;i<order_arr.size();i++)
			{
				map=new HashMap<String,String>();
				if(i!=0) select_str+=",";
				SQLSelectOrderByItem order_item=order_arr.get(i);
				expr=order_item.getExpr();
				if(expr instanceof SQLPropertyExpr)
				{
					map.put("name", ((SQLPropertyExpr)expr).getName());
					map.put("owner", ((SQLIdentifierExpr)((SQLPropertyExpr)expr).getOwner()).getName());

				}
				else if(expr instanceof SQLIdentifierExpr)
				{
					map.put("name", ((SQLIdentifierExpr)expr).getName());
				}
				else if(expr instanceof SQLAggregateExpr)
				{
					String method_name=((SQLAggregateExpr)expr).getMethodName();
					List<SQLExpr> arrs=((SQLAggregateExpr)expr).getArguments();
					Option option=((SQLAggregateExpr)expr).getOption();
					String option_name="";
					if(option!=null)
					{
						option_name=option.name();
					}
					String arg="";
					for(j=0;j<arrs.size();j++)
					{
						if(j!=0) 
						{
							arg+=",";
						}
						expr=arrs.get(j);
						if(expr  instanceof SQLPropertyExpr)
						{
							arg+=((SQLPropertyExpr) expr).getName();
						}
						else if(expr  instanceof SQLIdentifierExpr)
						{
							arg+=((SQLIdentifierExpr) expr).getName();
						}
						else
						{
							System.out.println(expr.toString()+" is "+expr.getClass());
						}
					}
					if(method_name.equalsIgnoreCase("avg"))
					{
						map.put("name", "SUM(AVG_SUM_"+arg+")/SUM(AVG_COUNT_"+arg+")");
					}
					else
					{
						map.put("name", method_name+"("+option_name+" "+arg+")");
					}

				}
				else
				{
					System.out.println("order column not support:"+expr.toString()+" is "+expr.getClass());
				}
				map.put("type", order_item.getType().name());
				order_str+=map.get("name")+" "+map.get("type");   	
				orderColumns.add(map);
			}
			// System.out.println("order:"+order_str);
		}


		//获得where
		expr=query.getWhere();
		if(expr!=null)
		{
			map=new HashMap<String,String>();
			map.put("left", ((SQLBinaryOpExpr)expr).left.toString());
			map.put("oper", ((SQLBinaryOpExpr)expr).operator.name);
			map.put("right", ((SQLBinaryOpExpr)expr).right.toString());
			whereColumns.add(map);
			//System.out.println("conditon:"+map.get("left")+" "+map.get("oper")+" "+map.get("right"));
		}

		//获得limit
		Limit limit=query.getLimit();
		if(limit!=null)
		{
			expr=limit.getRowCount();
			if(expr instanceof SQLIntegerExpr)
			{
				selectLimit.put("rowcount", ((SQLIntegerExpr) expr).getNumber());
			}
			else
			{
				System.out.println("limit rowcount column not support:"+expr.toString()+" is "+expr.getClass());
			}
			expr=limit.getOffset();
			if(expr!=null)
			{
				if(expr instanceof SQLIntegerExpr)
				{
					selectLimit.put("offset", ((SQLIntegerExpr) expr).getNumber());
				}
				else
				{
					System.out.println("limit offset column not support:"+expr.toString()+" is "+expr.getClass());
				}
			}

		}






		// StringBuilder out = new StringBuilder();
		// MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
		//        MySqlASTVisitorAdapter visitor = new MySqlASTVisitorAdapter();
		//
		//        for (SQLStatement statement : statementList) {
		//            statement.accept(visitor);
		//        }
		// out.toString();
	}
	public String getDataNodeQuerySQL()
	{
		String sql="select ";


		HashMap<String,String> map;
		int i;
		if(hasCountDistinct)
		{
			sql+=" distinct ";
			for(i=0;i<selectColumns.size();i++)
			{
				if(i!=0) sql+=",";
				map=selectColumns.get(i);
				if(map.get("owner")!=null)
				{
					sql+=map.get("owner")+".";
				}
				sql+=map.get("name")+" as "+map.get("alias");

			}
		}
		else
		{
			for(i=0;i<selectColumns.size();i++)
			{
				if(i!=0) sql+=",";
				map=selectColumns.get(i);
				if(map.get("type").equalsIgnoreCase("measure"))
				{
					if(map.get("method").equalsIgnoreCase("avg"))
					{
						sql+="SUM(";
						if(map.get("owner")!=null)
						{
							sql+=map.get("owner")+".";
						}
						sql+=map.get("name")+")"+" as AVG_SUM_"+map.get("alias");
						sql+=",COUNT(";
						if(map.get("owner")!=null)
						{
							sql+=map.get("owner")+".";
						}
						sql+=map.get("name")+")"+" as AVG_COUNT_"+map.get("alias");
					}
					else if(map.get("method").equalsIgnoreCase("stddev_samp") || map.get("method").equalsIgnoreCase("stddev_pop") || map.get("method").equalsIgnoreCase("var_samp") || map.get("method").equalsIgnoreCase("var_pop"))
					{
						String str="";
						if(map.get("owner")!=null)
						{
							str+=map.get("owner")+".";
						}
						str+=map.get("name");
						sql+="SUM("+str+"*"+str+")"+" as VAR_SUM1_"+map.get("alias");
						sql+=",SUM("+str+")"+" as VAR_SUM2_"+map.get("alias");
						sql+=",COUNT("+str+")"+" as VAR_COUNT_"+map.get("alias");
					}
					else
					{
						sql+=map.get("method")+"(";
						if(map.get("owner")!=null)
						{
							sql+=map.get("owner")+".";
						}
						sql+=map.get("name")+")"+" as "+map.get("alias");
					}
					
				}
				else
				{
					if(map.get("owner")!=null)
					{
						sql+=map.get("owner")+".";
					}
					sql+=map.get("name")+" as "+map.get("alias");
				}

			}
		}
		
		sql+=" from ";
		map=fromTables.get(0);
		sql+=map.get("name")+" "+map.get("alias");

		if(whereColumns.size()>0)
		{
			map=whereColumns.get(0);
			sql+=" where "+map.get("left")+" "+map.get("oper")+" "+map.get("right");
		}
		if(!hasCountDistinct)
		{
			for(i=0;i<groupColumns.size();i++)
			{
				if(i==0) sql+=" group by ";
				else sql+=",";
				map=groupColumns.get(i);
				if(map.get("owner")!=null)
				{
					sql+=map.get("owner")+".";
				}
				sql+=map.get("name");
			}
		}
		
		if(!hasGroup && selectLimit.get("rowcount")!=null)
		{
			sql+=" limit "+selectLimit.get("offset")+","+selectLimit.get("rowcount");
		}
		System.out.println("DataNodeQuerySQL:"+sql);
		return sql;
	}
	public String getServerNodeQuerySQL()
	{
		String sql="select ";
		HashMap<String,String> map;
		int i;
		for(i=0;i<selectColumns.size();i++)
		{
			if(i!=0) sql+=",";
			map=selectColumns.get(i);
			if(map.get("type").equalsIgnoreCase("measure"))
			{
				if(map.get("method").equalsIgnoreCase("count"))
				{
					if(map.get("option")!=null && map.get("option").equalsIgnoreCase("distinct"))
					{
						sql+="COUNT( distinct "+map.get("alias")+") as "+map.get("alias");
					}
					else
					{
						sql+="SUM("+map.get("alias")+") as "+map.get("alias");
					}
					
				}
				else if(map.get("method").equalsIgnoreCase("avg") && !hasCountDistinct)
				{
					sql+="SUM(AVG_SUM_"+map.get("alias")+")/SUM(AVG_COUNT_"+map.get("alias")+") as "+map.get("alias");
				}
				else if(map.get("method").equalsIgnoreCase("var_samp"))
				{
					sql+="(SUM(VAR_SUM1_"+map.get("alias")+")-SUM(VAR_SUM2_"+map.get("alias")+")*SUM(VAR_SUM2_"+map.get("alias")+")/SUM(VAR_COUNT_"+map.get("alias")+"))/(SUM(VAR_COUNT_"+map.get("alias")+")-1) as "+map.get("alias");
				}
				else if(map.get("method").equalsIgnoreCase("var_pop"))
				{
					sql+="(SUM(VAR_SUM1_"+map.get("alias")+")-SUM(VAR_SUM2_"+map.get("alias")+")*SUM(VAR_SUM2_"+map.get("alias")+")/SUM(VAR_COUNT_"+map.get("alias")+"))/SUM(VAR_COUNT_"+map.get("alias")+") as "+map.get("alias");
				}
				else if(map.get("method").equalsIgnoreCase("stddev_samp"))
				{
					sql+="SQRT((SUM(VAR_SUM1_"+map.get("alias")+")-SUM(VAR_SUM2_"+map.get("alias")+")*SUM(VAR_SUM2_"+map.get("alias")+")/SUM(VAR_COUNT_"+map.get("alias")+"))/(SUM(VAR_COUNT_"+map.get("alias")+")-1)) as "+map.get("alias");
				}
				else if(map.get("method").equalsIgnoreCase("stddev_pop"))
				{
					sql+="SQRT((SUM(VAR_SUM1_"+map.get("alias")+")-SUM(VAR_SUM2_"+map.get("alias")+")*SUM(VAR_SUM2_"+map.get("alias")+")/SUM(VAR_COUNT_"+map.get("alias")+"))/SUM(VAR_COUNT_"+map.get("alias")+")) as "+map.get("alias");
				}
				else
				{
					sql+=map.get("method")+"("+map.get("alias")+") as "+map.get("alias");
				}

			}
			else
			{
				sql+=map.get("alias");
			}

		}
		sql+=" from ";
		//map=fromTables.get(0);
		//sql+=sql_id+"_"+map.get("alias");
		sql+=sql_id;
		for(i=0;i<groupColumns.size();i++)
		{
			if(i==0) sql+=" group by ";
			else sql+=",";
			map=groupColumns.get(i);
			sql+=map.get("name");
		}
		for(i=0;i<havingColumns.size();i++)
		{
			if(i==0) sql+=" having ";
			else sql+=",";
			map=havingColumns.get(i);
			sql+=map.get("left")+map.get("oper")+map.get("right");
		}
		for(i=0;i<orderColumns.size();i++)
		{
			if(i==0) sql+=" order by ";
			else sql+=",";
			map=orderColumns.get(i);
			sql+=map.get("name")+" "+map.get("type");
		}
		if(selectLimit.get("rowcount")!=null)
		{
			if(selectLimit.get("offset")!=null)
			{
				sql+=" limit "+selectLimit.get("offset")+","+selectLimit.get("rowcount");
			}
			else
			{
				sql+=" limit "+selectLimit.get("rowcount");
			}

		}
		System.out.println("ServerNodeQuerySQL:"+sql);
		return sql;
	}
	public HashMap<String,String> getServerNodeCreateSQL()
	{
		String tab_name="";
		String sql="create table ";
		HashMap<String,String> map;
		//map=fromTables.get(0);
		tab_name=sql_id;
		sql+=tab_name;
		sql+=" (";
		for(int i=0;i<selectColumns.size();i++)
		{
			if(i!=0) sql+=",";
			map=selectColumns.get(i);

			
			if(map.get("type").equalsIgnoreCase("measure")&&!hasCountDistinct)
			{
				if(map.get("method").equalsIgnoreCase("avg"))
				{
					sql+="AVG_SUM_"+map.get("alias")+" double,AVG_COUNT_"+map.get("alias")+" double";
				}
				else if(map.get("method").equalsIgnoreCase("stddev_samp") || map.get("method").equalsIgnoreCase("stddev_pop") || map.get("method").equalsIgnoreCase("var_samp") || map.get("method").equalsIgnoreCase("var_pop"))
				{
					sql+="VAR_SUM1_"+map.get("alias")+" double,VAR_SUM2_"+map.get("alias")+" double,VAR_COUNT_"+map.get("alias")+" double";
				}
				else
				{
					sql+=map.get("alias")+" double";
				}
			}
			else
			{
				sql+=map.get("alias")+" text";
			}
		}
		sql+=")";

		System.out.println("ServerNodeCreateSQL:"+sql);
		HashMap<String,String> result=new HashMap<String,String>();
		result.put("tab_name", tab_name);
		result.put("sql", sql);
		return result;
	}
	public static String convertString(String str)
	{
		String regEx = "[^0-9a-zA-Z]";
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		//替换与模式匹配的所有字符（即非数字的字符将被""替换）
		return m.replaceAll("_").trim();
	}

}
