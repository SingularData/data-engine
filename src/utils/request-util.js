import config from 'config';
import { sample } from 'lodash';

const userAgents = config.get('harvester.user_agents');

export function getOptions() {
  return {
    json: true,
    gzip: true,
    timeout: 30000,
    headers: {
      'User-Agent': sample(userAgents)
    }
  };
}
