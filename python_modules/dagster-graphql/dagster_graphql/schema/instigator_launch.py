import graphene
from dagster_graphql.implementation.loader import RepositoryScopedBatchLoader
from dagster_graphql.implementation.utils import capture_error, check_permission

import dagster._check as check
from dagster._core.host_representation import ExternalSensor, ExternalTargetData, SensorSelector
from dagster._core.scheduler.instigation import InstigatorState
from dagster._core.workspace.permissions import Permissions

from ..implementation.fetch_sensors import (
    get_sensor_next_tick,
    set_sensor_cursor,
    start_sensor,
)
from ..implementation.instigator_launch import test_instigator
from .asset_key import GrapheneAssetKey
from .errors import (
    GraphenePythonError,
    GrapheneRepositoryNotFoundError,
    GrapheneSensorNotFoundError,
    GrapheneUnauthorizedError,
)
from .inputs import GrapheneSensorSelector
from .instigation import GrapheneFutureInstigationTick, GrapheneInstigationState
from .util import non_null_list


class GrapheneTestInstigatorResult(graphene.ObjectType):
    # TODO: fill this out
    instigatorId = graphene.Field(graphene.String)

    class Meta:
        name = "GrapheneTestInstigatorResult"

    def __init__(self, instigator_state):
        super().__init__()
        self._instigator_state = check.inst_param(
            instigator_state, "instigator_state", InstigatorState
        )

    def resolve_testInstigator(self, _graphene_info):
        return GrapheneInstigationState(instigator_state=self._instigator_state)


class GrapheneInstigatorSelector(graphene.InputObjectType):
    # TODO: make this work with both schedules and sensors
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    instigatorName = graphene.NonNull(graphene.String)

    class Meta:
        description = """This type represents the fields necessary to identify a sensor."""
        name = "SensorSelector"


class GrapheneTestInstigatorMutation(graphene.Mutation):
    """Enable a sensor to launch runs for a job based on external state change."""

    Output = graphene.NonNull(GrapheneTestInstigatorResult)

    class Arguments:
        instigator_selector = graphene.NonNull(GrapheneInstigatorSelector)

    class Meta:
        name = "StartSensorMutation"

    @capture_error
    @check_permission(Permissions.EDIT_SENSOR)
    def mutate(self, graphene_info, instigator_selector):
        return test_instigator(
            graphene_info, SensorSelector.from_graphql_input(instigator_selector)
        )
