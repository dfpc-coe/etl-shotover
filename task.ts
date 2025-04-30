import { Type, TSchema } from '@sinclair/typebox';
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
        const layer = await this.fetchLayer()
        const env = await this.env(IncomingInput);

        for (const marker of env.AugmentedMarkers) {
            let lease = await this.fetch(`/api/connection/${layer.connection}/video/lease/${marker.LeaseID}`, {
                method: 'GET'
            }) as { read_user?: string, read_pass?: string, protocols: { rtsp?: { name: string, url: string } } }

            if (lease.read_user && lease.read_pass) {
                console.log(`ok - Rotating Read Password for ${marker.LeaseID}`);
                lease = await this.fetch(`/api/connection/${layer.connection}/video/lease/${marker.LeaseID}`, {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        secret_rotate: true
                    })
                }) as { read_user?: string, read_pass?: string, protocols: { rtsp?: { name: string, url: string } } }
            } else {
                console.log(`ok - Skipping Rotation for ${marker.LeaseID}`);
            }

            const injectors = await this.fetch('/api/server/injector', {
                headers: {
                    Authorization: `Bearer ${env.AdminToken}`
                }
            }) as { items: Array<{ uid: string, toInject: string }> };

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

            if (lease.protocols.rtsp) {
                await this.fetch(`/api/server/injector`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${env.AdminToken}`
                    },
                    body: JSON.stringify({
                        uid: marker.UID,
                        toInject: `__video url="${lease.protocols.rtsp.url.replace(/\{\{username\}\}/, lease.read_user).replace(/\{\{password\}\}/, lease.read_pass)}"`
                    })
                })
            }
        }
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

