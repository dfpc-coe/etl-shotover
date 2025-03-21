import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import CoT from '@tak-ps/node-cot';
import type Lambda from 'aws-lambda';
import ETL, { SchemaType, handler as internal, local, InputFeatureCollection, DataFlowType, InvocationType } from '@tak-ps/etl';

const IncomingInput = Type.Object({
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
})

const OutgoingInput = Type.Object({
    Passthrough: Type.Boolean({
        description: 'Pass unmatched COT markers through unchanged',
        default: true,
    }),
    AugmentedMarkers: Type.Array(Type.Object({
        UID: Type.String(),
        LeaseID: Type.String(),
        RotateReadCredsFreq: Type.String({
            enum: [
                'Never'
            ],
            default: 'Never'
        })
    }), {
        default: []
    })
})

export default class Task extends ETL {
    static name = 'etl-shotover'
    static flow = [ DataFlowType.Incoming, DataFlowType.Outgoing ];
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
            if (type === SchemaType.Input) {
                return OutgoingInput;
            } else {
                return Type.Object({});
            }
        }
    }

    async outgoing(event: Lambda.SQSEvent): Promise<boolean> {
        const layer = await this.fetchLayer();
        const env = await this.env(OutgoingInput, DataFlowType.Outgoing);

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        for (const record of event.Records) {
            const parsed = (JSON.parse(record.body) as {
                xml: string
            }).xml;

            const cot = new CoT(parsed);

            console.error('Recieved', cot.uid());

            for (const AugmentedMarker of env.AugmentedMarkers) {
                if (cot.uid() === AugmentedMarker.UID) {
                    console.error('MATCH', cot.uid, AugmentedMarker.UID);
                    const lease = await this.fetch(`/api/connection/${layer.connection}/video/lease/${AugmentedMarker.LeaseID}`);
                    console.error('LEASE', lease);
                }
            }

            fc.features.push(cot.to_geojson());
        }

        await this.submit(fc);

        return true;
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

