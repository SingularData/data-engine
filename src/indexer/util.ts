import * as _ from "lodash";

export function indexDatasets(es, datasets) {
  const body = [];

  for (let dataset of datasets) {
    const action = {
      index: {
        _index: process.env.ES_INDEX,
        _type: process.env.ES_DOC_TYPE,
        _id: dataset.dcat.identifier
      }
    };

    body.push(action, dataset);
  }

  return es.bulk({ body });
}
