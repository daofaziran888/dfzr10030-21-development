"""校验运行包与采集监控接力的接口级自检（标准库 unittest，直接调用服务层）。

运行：cd backend && .venv/bin/python -m unittest tests.test_validation_runs -v
"""
from __future__ import annotations

import unittest

from app.services import validation_runs as runs
from app.store import store


class ValidationRunFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        runs.reset_runs()
        # 每个用例用独立的规则表，避免互相污染。
        store._tables["validator"] = [
            {"id": 1, "规则编号": "T-01", "校验字段": "标题", "校验类型": "非空",
             "期望值": "", "容错范围": "0", "采样比例": "100%", "阻断级别": "阻断",
             "status": "已启用", "pending": False, "abnormal": False},
            {"id": 2, "规则编号": "T-02", "校验字段": "价格", "校验类型": "数值",
             "期望值": ">= 0", "容错范围": "0", "采样比例": "50%", "阻断级别": "阻断",
             "status": "已启用", "pending": False, "abnormal": False},
            {"id": 3, "规则编号": "T-03", "校验字段": "库存", "校验类型": "数值范围",
             "期望值": "0-100", "容错范围": "0", "采样比例": "100%", "阻断级别": "告警",
             "status": "已启用", "pending": False, "abnormal": False},
        ]
        runs.bump_rule_revision()

    def test_abnormal_samples_come_from_sample_scope(self) -> None:
        samples = [
            {"样本编号": "S1", "标题": "耳机", "价格": "10", "库存": "5"},
            {"样本编号": "S2", "标题": "", "价格": "20", "库存": "6"},
            {"样本编号": "S3", "标题": "键盘", "价格": "面议", "库存": "200"},
        ]
        run, message, _ = runs.create_run({"样本": samples, "采样比例": "100%"})
        self.assertIsNotNone(run, message)
        scope = set(run["采样范围"])
        for entry in run["异常样本清单"]:
            self.assertIn(entry["样本编号"], scope)
            self.assertIn(entry["规则编号"], run["规则范围"])
            self.assertTrue(entry["采样范围"])
        self.assertEqual(run["整体结论"], "阻断")
        self.assertFalse(run["整体通过"])
        self.assertTrue(run["下游阻断"])

    def test_blocking_rule_hit_never_reports_pass(self) -> None:
        samples = [{"样本编号": "S1", "标题": "", "价格": "10", "库存": "5"}]
        run, _, _ = runs.create_run({"样本": samples, "采样比例": "100%"})
        self.assertEqual(run["整体结论"], "阻断")
        self.assertFalse(run["整体通过"])

    def test_rule_level_sampling_ratio_applied(self) -> None:
        samples = [{"样本编号": f"S{i}", "标题": "t", "价格": "1", "库存": "1"} for i in range(10)]
        run, _, _ = runs.create_run({"样本": samples})
        by_rule = {item["规则编号"]: item for item in run["规则结果"]}
        self.assertEqual(by_rule["T-01"]["采样数量"], 10)
        self.assertEqual(by_rule["T-02"]["采样数量"], 5)
        self.assertEqual(run["采样数量"], 10)

    def test_empty_sample_is_undetermined_and_blocks_downstream(self) -> None:
        run, message, _ = runs.create_run({"样本": [], "采样比例": "100%"})
        self.assertEqual(run["整体结论"], "无法判定", message)
        self.assertFalse(run["整体通过"])
        self.assertTrue(run["下游阻断"])
        self.assertTrue(run["无法判定"])
        self.assertTrue(any("本次采样为空" in reason for reason in run["边界原因"]))

    def test_historical_block_shown_but_current_boundary_kept(self) -> None:
        first, _, _ = runs.create_run({
            "样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}],
            "采样比例": "100%",
        })
        self.assertEqual(first["整体结论"], "阻断")
        empty, _, _ = runs.create_run({"样本": [], "采样比例": "100%"})
        self.assertEqual(empty["整体结论"], "无法判定")
        self.assertTrue(empty["历史阻断依据"])
        self.assertTrue(empty["历史风险"])
        self.assertTrue(any("历史风险" in reason for reason in empty["边界原因"]))
        self.assertFalse(empty["整体通过"])

    def test_idempotent_run_does_not_double_write(self) -> None:
        payload = {"样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}], "requestId": "REQ-1"}
        first, _, reused_first = runs.create_run(payload)
        second, _, reused_second = runs.create_run(payload)
        self.assertFalse(reused_first)
        self.assertTrue(reused_second)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(runs.list_runs()[0]), 1)

    def test_stale_version_is_rejected(self) -> None:
        current = runs.current_rule_revision()
        run, message, _ = runs.create_run({"expectedVersion": f"R{current - 1}", "样本": []})
        self.assertIsNone(run)
        self.assertIn("数据版本冲突", message)
        self.assertEqual(runs.list_runs()[0], [])

    def test_legacy_values_fall_back_safely(self) -> None:
        store._tables["validator"] = [{
            "id": 9, "规则编号": "T-OLD", "校验字段": "标题", "校验类型": "未知类型",
            "期望值": "", "容错范围": "", "采样比例": "很高", "阻断级别": "很重要",
            "status": "已启用", "pending": False, "abnormal": False,
        }]
        runs.bump_rule_revision()
        run, message, _ = runs.create_run({
            "规则编号": ["T-OLD"],
            "样本": [{"样本编号": "S1", "标题": ""}],
            "采样比例": "按规则配置",
        })
        self.assertIsNotNone(run, message)
        rule = run["规则结果"][0]
        self.assertTrue(rule["旧口径兼容"])
        self.assertEqual(rule["采样数量"], 1)
        self.assertEqual(run["口径"], "旧口径兼容")
        # 阻断级别无法识别时按阻断处理，异常样本仍然不能显示为通过。
        self.assertEqual(run["整体结论"], "阻断")

    def test_disabled_rules_are_not_executed(self) -> None:
        store._tables["validator"][0]["status"] = "已停用"
        runs.bump_rule_revision()
        run, _, _ = runs.create_run({"样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}]})
        self.assertNotIn("T-01", run["规则范围"])

    def test_concurrent_submission_keeps_single_result(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        payload = {"样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}],
                   "requestId": "REQ-RACE"}
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: runs.create_run(payload)[0], range(8)))
        self.assertEqual(len({item["id"] for item in results}), 1)
        self.assertEqual(len(runs.list_runs()[0]), 1)
        first, _ = runs.relay_run(results[0]["id"])
        second, _ = runs.relay_run(results[0]["id"])
        self.assertEqual(first["监控记录"]["记录编号"], second["监控记录"]["记录编号"])
        self.assertEqual(len(runs.list_todos()), 1)

    def test_monitor_relay_writes_todos_once(self) -> None:
        run, _, _ = runs.create_run({
            "样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}],
            "采样比例": "100%",
        })
        result, message = runs.relay_run(run["id"])
        self.assertIsNotNone(result, message)
        self.assertGreater(result["新增待办数"], 0)
        again, _ = runs.relay_run(run["id"])
        self.assertEqual(again["新增待办数"], result["新增待办数"])
        todos = runs.list_todos()
        self.assertEqual(len(todos), result["新增待办数"])
        for todo in todos:
            self.assertEqual(todo["来源运行包"], run["运行包编号"])
            self.assertTrue(todo["校验规则"])
            self.assertTrue(todo["数据版本"])

    def test_monitor_status_counts_abnormal_and_undetermined(self) -> None:
        runs.create_run({"样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}],
                         "采样比例": "100%"})
        runs.create_run({"样本": [], "采样比例": "100%"})
        runs.relay_all()
        status = runs.monitor_status()
        self.assertGreaterEqual(status["当日告警"], 2)
        self.assertGreater(status["阻断数量"], 0)
        self.assertEqual(status["无法判定数"], 1)

    def test_monitor_writeback_respects_newer_rule_version(self) -> None:
        run, _, _ = runs.create_run({"样本": [{"样本编号": "S1", "标题": "ok", "价格": "1", "库存": "1"}]})
        runs.bump_rule_revision()  # 规则已更新，旧运行包不得覆盖
        result, _ = runs.relay_run(run["id"])
        self.assertTrue(result["规则差异"])
        self.assertEqual(result["规则差异"][0]["状态"], "未写回")
        self.assertIn("数据版本", result["规则差异"][0]["原因"])

    def test_monitor_writeback_applies_for_current_version(self) -> None:
        run, _, _ = runs.create_run({"样本": [{"样本编号": "S1", "标题": "", "价格": "1", "库存": "1"}]})
        result, _ = runs.relay_run(run["id"])
        written = [item for item in result["规则差异"] if item["状态"] == "已写回"]
        self.assertTrue(written)
        rule = next(row for row in store.rows("validator") if row["规则编号"] == "T-01")
        self.assertEqual(rule["最近运行包"], run["运行包编号"])
        self.assertEqual(rule["最近阻断数"], 1)


if __name__ == "__main__":
    unittest.main()
