import { FirestoreClient } from "./FirestoreClient.js";
import { settings } from "../../../shared/config.js";
import { Template } from "../../../shared/models/template.js";

export class FirestoreTemplateRepository {
  private readonly client: FirestoreClient;

  constructor(client?: FirestoreClient) {
    this.client = client ?? FirestoreClient.getInstance();
  }

  async findAll(): Promise<Template[]> {
    const snap = await this.client.firestore.collection(settings.TEMPLATE_COLLECTION).get();
    return snap.docs.map((doc) => doc.data() as Template);
  }

  async save(template: Template): Promise<void> {
    await this.client.firestore
      .collection(settings.TEMPLATE_COLLECTION)
      .doc(template.template_id)
      .set(template);
  }

  async updateMatchCount(templateId: string, matchCount: number, updatedAt: string): Promise<void> {
    await this.client.firestore
      .collection(settings.TEMPLATE_COLLECTION)
      .doc(templateId)
      .update({ match_count: matchCount, updated_at: updatedAt });
  }
}
