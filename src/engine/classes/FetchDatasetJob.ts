import { IJob } from "../services/job-queue";
import uuid = require("uuid/v1");

export class FetchDatasetJob implements IJob {
  public type: string;
  public messageId: string;
  public data: { sourceType: string; url: string };

  constructor(sourceType: string, url: string) {
    this.type = "FetchDataset";
    this.messageId = uuid();
    this.data = { sourceType, url };
  }
}
