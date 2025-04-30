import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import CoT from '@tak-ps/node-cot';
import type Lambda from 'aws-lambda';
import ETL, { SchemaType, handler as internal, local, InputFeatureCollection, DataFlowType, InvocationType } from '@tak-ps/etl';

const IncomingInput = Type.Object({
    DEBUG: Type.Boolean({
        default: false,
        description: 'Print results in logs'
    }),
    AdminToken: Type.String(),
    AugmentedMarkers: Type.Array(Type.Object({
        UID: Type.String(),
        LeaseID: Type.String(),
    }), {
        default: []
    })
})

export default class Task extends ETL {
    static name = 'etl-shotover'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return IncomingInput;
            } else {
                return Type.Object({});
            }
       } else if (flow === DataFlowType.Outgoing) {
            return Type.Object({});
        }
    }

    async control() {
/*
        cot.addVideo({
            uid: cot.uid() + '-video',
            url: lease.protocols.rtsp.url
                .replace('{{mode}}', 'read')
                .replace('{{username}}', lease.lease.read_user)
                .replace('{{password}}', lease.lease.read_pass)
        });
*/
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

