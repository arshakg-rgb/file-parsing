import { FirestoreClient } from "./FirestoreClient.js";
import { settings } from "@shared/Settings.js";
import { Template } from "@shared/models/template.js";

/**
 * FirestoreTemplateRepository is responsible for firestore template repository operations.
 */
export class FirestoreTemplateRepository {
    /**
   * Client
   * @private
   */
  private readonly client: FirestoreClient;

    /**
   * Constructs a new FirestoreTemplateRepository instance.
   * @param client - The client
   */
  constructor(client?: FirestoreClient) {
    this.client = client ?? FirestoreClient.getInstance();
  }

    /**
   * Finds all
   * @returns A promise that resolves to the list
   */
  async findAll(): Promise<Template[]> {
    const snap = await this.client.firestore.collection(settings.TEMPLATE_COLLECTION).get();
    return snap.docs.map((doc) => doc.data() as Template);
  }

    /**
   * Saves the operation
   * @param template - The template
   */
  async save(template: Template): Promise<void> {
    await this.client.firestore
      .collection(settings.TEMPLATE_COLLECTION)
      .doc(template.template_id)
      .set(template);
  }

    /**
   * Updates match count
   * @param templateId - The template id
   * @param matchCount - The match count
   * @param updatedAt - The updated at
   */
  async updateMatchCount(templateId: string, matchCount: number, updatedAt: string): Promise<void> {
    await this.client.firestore
      .collection(settings.TEMPLATE_COLLECTION)
      .doc(templateId)
      .update({ match_count: matchCount, updated_at: updatedAt });
  }
}
