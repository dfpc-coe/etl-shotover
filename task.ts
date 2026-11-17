import { Type, TSchema, Static } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

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

const Lease = Type.Object({
    lease: Type.Object({
        read_user: Type.Optional(Type.Union([Type.Null(), Type.String()])),
        read_pass: Type.Optional(Type.Union([Type.Null(), Type.String()]))
    }),
    protocols: Type.Object({
        rtsp: Type.Optional(Type.Union([Type.Null(), Type.Object({
            url: Type.String()
        })]))
    })
});

const Injectors = Type.Object({
    items: Type.Array(Type.Object({
        uid: Type.String(),
        toInject: Type.String()
    }))
});

function typed<T extends TSchema>(schema: T, body: unknown): Static<T> {
    if (!Value.Check(schema, body)) throw new Error('Unexpected API response');

    return body;
}

export default class Task extends ETL {
    static name = 'etl-shotover'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming && type === SchemaType.Input) return IncomingInput;

        return Type.Object({});
    }

    async control() {
        const layer = await this.fetchLayer()
        const env = await this.env(IncomingInput);

        const errors: Error[] = [];

        for (const marker of env.AugmentedMarkers) {
            try {
                let lease = typed(Lease, await this.fetch(`/api/connection/${layer.connection}/video/lease/${marker.LeaseID}`, {
                    method: 'GET'
                }));

                if (lease.lease.read_user && lease.lease.read_pass) {
                    console.log(`ok - Rotating Read Password for ${marker.LeaseID}`);
                    lease = typed(Lease, await this.fetch(`/api/connection/${layer.connection}/video/lease/${marker.LeaseID}`, {
                        method: 'PATCH',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            secure_rotate: true
                        })
                    }));
                } else {
                    console.log(`ok - Skipping Rotation for ${marker.LeaseID}`);
                }

                const injectors = typed(Injectors, await this.fetch('/api/server/injector', {
                    headers: {
                        Authorization: `Bearer ${env.AdminToken}`
                    }
                }));

                for (const injector of injectors.items) {
                    if (injector.uid === marker.UID) {
                        console.log(`ok - Deleting Old Injector for ${marker.UID}`);

                        await this.fetch(`/api/server/injector?uid=${encodeURIComponent(injector.uid)}&toInject=${encodeURIComponent(injector.toInject)}`, {
                            method: 'DELETE',
                            headers: {
                                Authorization: `Bearer ${env.AdminToken}`
                            }
                        })
                    }
                }

                if (lease.protocols.rtsp && lease.lease.read_user && lease.lease.read_pass) {
                    await this.fetch(`/api/server/injector`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            Authorization: `Bearer ${env.AdminToken}`
                        },
                        body: JSON.stringify({
                            uid: marker.UID,
                            toInject: `__video url="${lease.protocols.rtsp.url.replace(/\{\{username\}\}/, lease.lease.read_user).replace(/\{\{password\}\}/, lease.lease.read_pass)}"`
                        })
                    })
                }
            } catch (err) {
                errors.push(err instanceof Error ? err : new Error(String(err)));
            }
        }

        if (errors.length > 0) {
            for (const error of errors) {
                console.error(error);
            }
        }

        throw new Error(`${errors.length} errors occurred during processing.`);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}

