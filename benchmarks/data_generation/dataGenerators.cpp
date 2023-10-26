#include <vector>
#include <random>
#include <algorithm>

#include "dataGenerators.hpp"


std::vector<int> generateClusteringOrder(int n, int spreadInCluster) {
    int numBuckets = 1 + n - spreadInCluster;
    std::vector<std::vector<int>> buckets(numBuckets, std::vector<int>());

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    for (int i = 0; i < n; i++) {
        std::uniform_int_distribution<int> distribution(std::max(0, 1 + i - spreadInCluster), std::min(i, n - spreadInCluster));
        buckets[distribution(gen)].push_back(i);
    }

    std::vector<int> result;
    for (int i = 0; i < numBuckets; i++) {
        std::shuffle(buckets[i].begin(), buckets[i].end(), gen);
        result.insert(result.end(), buckets[i].begin(), buckets[i].end());
    }

    return result;
}