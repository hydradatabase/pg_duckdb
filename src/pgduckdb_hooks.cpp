extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "optimizer/optimizer.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pgduckdb_pg_list.h"
#include "pgduckdb/pgduckdb_table_am.hpp"

static planner_hook_type PrevPlannerHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

static bool
is_duckdb_extension_registered() {
	return get_extension_oid("duckdb", true) != InvalidOid;
}

static bool
is_catalog_table(List *tables) {
	ListCell *lc;
	foreach (lc, tables) {
		RangeTblEntry *table = (RangeTblEntry *)lfirst(lc);
		if (table->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (is_catalog_table(table->subquery->rtable)) {
				return true;
			}
		}
		if (table->relid) {
			auto rel = RelationIdGetRelation(table->relid);
			auto namespaceOid = RelationGetNamespace(rel);
			if (namespaceOid == PG_CATALOG_NAMESPACE || namespaceOid == PG_TOAST_NAMESPACE) {
				RelationClose(rel);
				return true;
			}
			RelationClose(rel);
		}
	}
	return false;
}

static bool
contains_duckdb_table(List *rte_list) {
	foreach_node(RangeTblEntry, rte, rte_list) {
		if (rte->rtekind == RTE_SUBQUERY) {
			/* Check Subquery rtable list if any table is from PG catalog */
			if (contains_duckdb_table(rte->subquery->rtable)) {
				return true;
			}
		}
		if (rte->relid) {
			auto rel = RelationIdGetRelation(rte->relid);
			if (is_duckdb_table_am(rel->rd_tableam)) {
				RelationClose(rel);
				return true;
			}
			RelationClose(rel);
		}
	}
	return false;
}

static bool
needs_duckdb_execution(Query *parse) {
	if (parse->commandType == CMD_UTILITY) {
		return false;
	}
	return contains_duckdb_table(parse->rtable);
}

static bool
is_allowed_statement() {
	/* For `SELECT ..` ActivePortal doesn't exist */
	if (!ActivePortal)
		return true;
	/* `EXPLAIN ...` should be allowed */
	if (ActivePortal->commandTag == CMDTAG_EXPLAIN)
		return true;
	return false;
}

static PlannedStmt *
duckdb_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
	if (is_duckdb_extension_registered()) {
		if (duckdb_execution && is_allowed_statement() && parse->rtable && !is_catalog_table(parse->rtable) &&
		    parse->commandType == CMD_SELECT) {
			PlannedStmt *duckdbPlan = duckdb_plan_node(parse, query_string, cursorOptions, boundParams);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}

		if (needs_duckdb_execution(parse)) {
			PlannedStmt *duckdbPlan = duckdb_plan_node(parse, query_string, cursorOptions, boundParams);
			if (duckdbPlan) {
				return duckdbPlan;
			}
		}
	}

	if (PrevPlannerHook) {
		return PrevPlannerHook(parse, query_string, cursorOptions, boundParams);
	} else {
		return standard_planner(parse, query_string, cursorOptions, boundParams);
	}
}

static void
duckdb_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree, ProcessUtilityContext context,
               ParamListInfo params, struct QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (duckdb_execution && is_duckdb_extension_registered() && IsA(parsetree, CopyStmt)) {
		uint64 processed;
		if (duckdb_copy(pstmt, queryString, queryEnv, &processed)) {
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	if (is_duckdb_extension_registered()) {
		duckdb_handle_ddl(parsetree, queryString);
	}

	if (PrevProcessUtilityHook) {
		(*PrevProcessUtilityHook)(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	} else {
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	}
}

void
duckdb_init_hooks(void) {
	PrevPlannerHook = planner_hook;
	planner_hook = duckdb_planner;

	PrevProcessUtilityHook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = duckdb_utility;
}
