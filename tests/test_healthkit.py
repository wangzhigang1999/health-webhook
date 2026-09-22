"""healthkit.flatten 单元测试。"""

from health_webhook.healthkit import flatten
from health_webhook.models import HealthPayload


def test_flatten_quantity_and_category() -> None:
    payload = HealthPayload.model_validate(
        {
            "schemaVersion": "v1",
            "batchId": "b1",
            "deviceId": "dev1",
            "batches": [
                {
                    "hkTypeId": "HKQuantityTypeIdentifierHeartRate",
                    "samples": [
                        {
                            "uuid": "h1",
                            "startUnixMs": "1790084201021",
                            "endUnixMs": "1790084201021",
                            "quantity": {"value": 74.0, "unit": "count/min"},
                        }
                    ],
                },
                {
                    "hkTypeId": "HKCategoryTypeIdentifierSleepAnalysis",
                    "samples": [
                        {
                            "uuid": "s1",
                            "startUnixMs": "1790080000000",
                            "endUnixMs": "1790083600000",
                            "category": {"value": 1, "valueName": "asleep"},
                        }
                    ],
                },
            ],
        }
    )

    rows = flatten(payload)
    assert len(rows) == 2

    heart_rate, sleep = rows
    assert heart_rate.metric_type == "HKQuantityTypeIdentifierHeartRate"
    assert heart_rate.value == 74.0
    assert heart_rate.unit == "count/min"
    assert heart_rate.start_time == 1790084201021
    assert sleep.metric_type == "HKCategoryTypeIdentifierSleepAnalysis"
    assert sleep.category == "asleep"
    assert sleep.value == 1.0


def test_flatten_skips_route_samples() -> None:
    payload = HealthPayload.model_validate(
        {
            "batches": [
                {
                    "hkTypeId": "HKWorkoutRouteTypeIdentifier",
                    "samples": [{"uuid": "r1", "route": {"workoutUuid": "w1", "points": []}}],
                }
            ],
        }
    )
    assert flatten(payload) == []
