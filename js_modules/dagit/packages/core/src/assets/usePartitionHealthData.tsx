import {useApolloClient} from '@apollo/client';
import isEqual from 'lodash/isEqual';
import React from 'react';

import {graphql} from '../graphql';
import {
  PartitionHealthMaterializedPartitionsFragment,
  PartitionHealthQueryQuery,
  PartitionHealthQueryQueryVariables,
} from '../graphql/graphql';
import {PartitionState} from '../partitions/PartitionStatus';

import {isTimeseriesDimension, mergedStates} from './MultipartitioningSupport';
import {AssetKey} from './types';

/**
 * usePartitionHealthData retrieves partitionKeysByDimension + partitionMaterializationCounts and
 * reshapes the data for rapid retrieval from the UI. The hook exposes a series of getter methods
 * for each asset's data, hiding the underlying data structures from the rest of the app.
 *
 * The hope is that if we want to add support for 3- and 4- dimension partitioned assets, all
 * of the changes will be in this file. The rest of the app already supports N dimensions.
 */

export interface PartitionHealthData {
  assetKey: AssetKey;
  dimensions: PartitionHealthDimension[];
  stateForKey: (dimensionKeys: string[]) => PartitionState;
  stateForSingleDimension: (
    dimensionIdx: number,
    dimensionKey: string,
    otherDimensionSelectedKeys?: string[],
  ) => PartitionState;
}

export interface PartitionHealthDimension {
  name: string;
  partitionKeys: string[];
}

export type PartitionHealthDimensionRange = {
  dimension: PartitionHealthDimension;
  selected: string[];
};

export function buildPartitionHealthData(data: PartitionHealthQueryQuery, loadKey: AssetKey) {
  const __dims =
    data.assetNodeOrError.__typename === 'AssetNode'
      ? data.assetNodeOrError.partitionKeysByDimension
      : [];

  // The backend re-orders the dimensions only for the materializedPartitions ranges so that
  // the time partition is the "primary" one, even if it's dimension[1] elsewhere.
  // This matches the way we display them in the UI and makes some common data retrieval faster,
  // but Dagit's internals always use the REAL ordering of the partition keys, we need to flip
  // everything in this function to match the range data.
  const isRangeDataInverted = __dims.length === 2 && isTimeseriesDimension(__dims[1]);
  const dimensions = isRangeDataInverted ? [__dims[1], __dims[0]] : __dims;

  const ranges = addIndexes(
    dimensions,
    (data.assetNodeOrError.__typename === 'AssetNode' &&
      data.assetNodeOrError.materializedPartitions) || {
      __typename: 'MaterializedPartitions1D',
      ranges: [],
    },
  );

  console.log(ranges);

  const stateForKey = (dimensionKeys: string[]): PartitionState => {
    return stateForKeyRangeOrdering(isRangeDataInverted ? dimensionKeys.reverse() : dimensionKeys);
  };

  const stateForKeyRangeOrdering = (dimensionKeys: string[]): PartitionState => {
    const dIndexes = dimensionKeys.map((key, idx) => dimensions[idx].partitionKeys.indexOf(key));
    const d0Range = ranges.find((r) => r.start.idx <= dIndexes[0] && r.end.idx >= dIndexes[0]);
    if (!d0Range) {
      return PartitionState.MISSING;
    }
    if (!d0Range.subranges) {
      return PartitionState.SUCCESS;
    }
    const d1Range = d0Range.subranges.find(
      (r) => r.start.idx <= dIndexes[1] && r.end.idx >= dIndexes[1],
    );
    return d1Range ? PartitionState.SUCCESS : PartitionState.MISSING;
  };

  const stateForSingleDimension = (
    dimensionIdx: number,
    dimensionKey: string,
    otherDimensionSelectedKeys?: string[], // using this feature is slow
  ) => {
    if (isRangeDataInverted) {
      dimensionIdx = 1 - dimensionIdx;
    }
    if (dimensionIdx === 0 && dimensions.length === 1) {
      return stateForKeyRangeOrdering([dimensionKey]);
    }
    if (dimensionIdx === 0) {
      if (!otherDimensionSelectedKeys) {
        const d0Idx = dimensions[0].partitionKeys.indexOf(dimensionKey);
        const d0Range = ranges.find((r) => r.start.idx <= d0Idx && r.end.idx >= d0Idx);
        return d0Range?.value || PartitionState.MISSING;
      } else {
        return mergedStates(
          otherDimensionSelectedKeys.map((k) => stateForKeyRangeOrdering([dimensionKey, k])),
        );
      }
    }
    if (dimensionIdx === 1) {
      const d0Idxs = otherDimensionSelectedKeys?.map((key) =>
        dimensions[0].partitionKeys.indexOf(key),
      );
      const d1Idx = dimensions[1].partitionKeys.indexOf(dimensionKey);

      const d0RangesContainingSubrangeWithD1Idx = ranges.filter((r) =>
        r.subranges!.some((sr) => sr.start.idx <= d1Idx && sr.end.idx >= d1Idx),
      );

      const all = !d0Idxs
        ? d0RangesContainingSubrangeWithD1Idx.length === ranges.length
        : d0Idxs.every((i) =>
            d0RangesContainingSubrangeWithD1Idx.some((r) => r.start.idx <= i && r.end.idx >= i),
          );

      if (all) {
        return PartitionState.SUCCESS;
      } else if (d0RangesContainingSubrangeWithD1Idx.length > 0) {
        return PartitionState.SUCCESS_MISSING;
      }
      return PartitionState.MISSING;
    }
    throw new Error('stateForSingleDimension asked for third dimension');
  };

  const result: PartitionHealthData = {
    assetKey: loadKey,
    stateForKey,
    stateForSingleDimension,
    dimensions: __dims.map((d) => ({
      name: d.name,
      partitionKeys: d.partitionKeys,
    })),
  };

  return result;
}

// Temporary: Add indexes to the materializedPartitions data so that we can work with it
// without having to look up key positions. Note: This is pretty slow operating on such
// a large array.
type Range = {
  start: {key: string; idx: number};
  end: {key: string; idx: number};
  value: PartitionState.SUCCESS | PartitionState.SUCCESS_MISSING;
  subranges?: Range[];
};

function addIndexes(
  dimensions: {
    name: string;
    partitionKeys: string[];
  }[],
  materializedPartitions: PartitionHealthMaterializedPartitionsFragment,
) {
  const result: Range[] = [];

  for (const range of materializedPartitions.ranges) {
    if (range.__typename === 'MaterializedPartitionRange1D') {
      result.push({
        value: PartitionState.SUCCESS,
        start: {key: range.start, idx: dimensions[0].partitionKeys.indexOf(range.start)},
        end: {key: range.end, idx: dimensions[0].partitionKeys.indexOf(range.end)},
      });
    } else {
      const subranges: Range[] = range.secondaryDimRanges.map((sub) => {
        return {
          value: PartitionState.SUCCESS,
          start: {key: sub.start, idx: dimensions[1].partitionKeys.indexOf(sub.start)},
          end: {key: sub.end, idx: dimensions[1].partitionKeys.indexOf(sub.end)},
        };
      });
      console.log(subranges[0], dimensions[1]);
      result.push({
        value:
          subranges.length === 1 &&
          subranges[0].start.idx === 0 &&
          subranges[0].end.idx === dimensions[1].partitionKeys.length - 1
            ? PartitionState.SUCCESS
            : PartitionState.SUCCESS_MISSING,
        subranges,
        start: {
          key: range.primaryDimStart,
          idx: dimensions[0].partitionKeys.indexOf(range.primaryDimStart),
        },
        end: {
          key: range.primaryDimEnd,
          idx: dimensions[0].partitionKeys.indexOf(range.primaryDimEnd),
        },
      });
    }
  }

  return result;
}

// Note: assetLastMaterializedAt is used as a "hint" - if the input value changes, it's
// a sign that we should invalidate and reload previously loaded health stats. We don't
// clear them immediately to avoid an empty state.
//
export function usePartitionHealthData(assetKeys: AssetKey[], assetLastMaterializedAt = '') {
  const [result, setResult] = React.useState<(PartitionHealthData & {fetchedAt: string})[]>([]);
  const client = useApolloClient();

  const assetKeyJSONs = assetKeys.map((k) => JSON.stringify(k));
  const assetKeyJSON = JSON.stringify(assetKeyJSONs);
  const missingKeyJSON = assetKeyJSONs.find(
    (k) =>
      !result.some(
        (r) => JSON.stringify(r.assetKey) === k && r.fetchedAt === assetLastMaterializedAt,
      ),
  );

  React.useMemo(() => {
    if (!missingKeyJSON) {
      return;
    }
    const loadKey: AssetKey = JSON.parse(missingKeyJSON);
    const run = async () => {
      const {data} = await client.query<
        PartitionHealthQueryQuery,
        PartitionHealthQueryQueryVariables
      >({
        query: PARTITION_HEALTH_QUERY,
        fetchPolicy: 'network-only',
        variables: {
          assetKey: {path: loadKey.path},
        },
      });
      const loaded = buildPartitionHealthData(data, loadKey);
      setResult((result) => [
        ...result.filter((r) => !isEqual(r.assetKey, loadKey)),
        {...loaded, fetchedAt: assetLastMaterializedAt},
      ]);
    };
    run();
  }, [client, missingKeyJSON, assetLastMaterializedAt]);

  return React.useMemo(() => {
    const assetKeyJSONs = JSON.parse(assetKeyJSON);
    return result.filter((r) => assetKeyJSONs.includes(JSON.stringify(r.assetKey)));
  }, [assetKeyJSON, result]);
}

const PARTITION_HEALTH_QUERY = graphql(`
  query PartitionHealthQuery($assetKey: AssetKeyInput!) {
    assetNodeOrError(assetKey: $assetKey) {
      ... on AssetNode {
        id
        partitionKeysByDimension {
          name
          partitionKeys
        }
        materializedPartitions {
          ...PartitionHealthMaterializedPartitions
        }
      }
    }
  }

  fragment PartitionHealthMaterializedPartitions on MaterializedPartitions {
    ... on MaterializedPartitions1D {
      ranges {
        start
        end
      }
    }
    ... on MaterializedPartitions2D {
      ranges {
        primaryDimStart
        primaryDimEnd
        secondaryDimRanges {
          start
          end
        }
      }
    }
  }
`);
